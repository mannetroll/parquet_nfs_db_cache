from collections.abc import Callable, Mapping
from datetime import UTC, datetime
import errno
import functools
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import shutil
import socket
import threading
import time
import uuid

import pyarrow.parquet as pq

CACHE_METADATA_VERSION = 1
CACHE_WRITER_VERSION = "nfscache.v1"
LOCK_METADATA_VERSION = 1
SQL_CACHE_FINGERPRINT_HEX_LENGTH = 16

import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

class _LockLease:
    def __init__(
        self,
        path: Path,
        stop_event: threading.Event,
        thread: threading.Thread,
    ) -> None:
        self.path = path
        self.stop_event = stop_event
        self.thread = thread


class NFSCache:
    # Cache-key version stamp for a table source. ORA_ROWSCN advances on any
    # change to a row's block, so MAX(ORA_ROWSCN) is a cheap version token; the
    # row count guards against table swaps that do not advance the SCN.
    VERSION_SQL = (
        "SELECT COUNT(*) AS N_ROWS, MAX(ORA_ROWSCN) AS MAX_ROWSCN FROM {table}"
    )
    _FROM_RE = re.compile(r"\bfrom\s+([A-Za-z0-9_$#.\"]+)", re.IGNORECASE)

    def __init__(
        self,
        cache_dir: Path,
        *,
        poll_seconds: float = 0.1,
        stale_lock_seconds: float = 900.0,
        heartbeat_seconds: float | None = None,
        connect_factory: Callable[[], object] | None = None,
        verify_checksum: bool = False,
        acquire_timeout_seconds: float | None = None,
    ) -> None:
        self.cache_dir = cache_dir
        self.poll_seconds = poll_seconds
        if stale_lock_seconds <= 0:
            raise ValueError("stale_lock_seconds must be positive")
        self.stale_lock_seconds = stale_lock_seconds
        if heartbeat_seconds is None:
            heartbeat_seconds = max(poll_seconds, min(5.0, stale_lock_seconds / 3))
        if heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        self.heartbeat_seconds = heartbeat_seconds
        # Opaque callable returning a DB connection (anything with a
        # context-manager `cursor()`); used by `sql_parquet` to read the
        # source version. Kept generic so the cache does not depend on oracledb.
        self.connect_factory = connect_factory
        # When True, every cache validation re-reads the whole parquet to verify
        # its SHA-256. That is an O(file size) read on each warm hit, so it is
        # off by default: atomic os.replace plus the size and parquet-footer
        # checks already pin the committed file's identity. Enable it only when
        # guarding against silent on-disk corruption matters more than warm-read
        # latency.
        self.verify_checksum = verify_checksum
        # Lock staleness compares a lock file's mtime (stamped by the file
        # server) against "now". Using this client's wall clock there makes the
        # age wrong by exactly the client/server clock skew, which across many
        # NFS/SMB hosts can wrongly break live locks or never break dead ones. We
        # instead learn our offset from the server clock by reading the mtime the
        # server assigns to files we write, and correct "now" by it.
        self._clock_offset = 0.0
        self._clock_offset_at: float | None = None
        self._clock_probe_interval = max(
            self.heartbeat_seconds, min(self.stale_lock_seconds, 30.0)
        )
        # Optional safety net: bound how long a lock acquisition may spin so a
        # wedged share or a permanently-held lock surfaces as TimeoutError
        # instead of hanging forever. None keeps the original unbounded wait.
        if acquire_timeout_seconds is not None and acquire_timeout_seconds <= 0:
            raise ValueError("acquire_timeout_seconds must be positive")
        self.acquire_timeout_seconds = acquire_timeout_seconds
        # Cached so the value written into lock.json matches what same-host
        # liveness checks compare against.
        self._hostname = socket.gethostname()

    def sql_parquet[**P](
        self,
        func: Callable[P, object],
    ) -> Callable[P, Path]:
        signature = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Path:
            args_tuple = tuple(args)
            kwargs_dict = dict(kwargs)
            sql = self._sql_arg(signature, args_tuple, kwargs_dict)
            output_path = Path(
                self._path_arg(signature, args_tuple, kwargs_dict)
            )
            normalized_sql = self._normalize_sql(str(sql))
            return_cols = kwargs_dict.get("return_cols")
            display_key = self._sql_display_key(normalized_sql, return_cols)

            def write_part(part_path: Path) -> None:
                part_args, part_kwargs = self._replace_path_arg(
                    signature,
                    args_tuple,
                    kwargs_dict,
                    part_path,
                )
                func(*part_args, **part_kwargs)

            def export(validated_path: Path) -> Path:
                # Runs while _run_cached_parquet still holds the cache lock, so
                # the validated file cannot be replaced or removed mid-copy.
                self._copy_cached_parquet(validated_path, output_path)
                return output_path

            return self._run_cached_parquet(
                display_key,
                lambda: self._sql_source_version(normalized_sql),
                write_part,
                source_sql=normalized_sql,
                export_fn=export,
            )

        return wrapper

    def _run_cached_parquet(
        self,
        display_key: str,
        version_fn: Callable[[], str | None],
        write_fn: Callable[[Path], None],
        *,
        source_sql: str | None,
        export_fn: Callable[[Path], Path],
    ) -> Path:
        cache_path = self._cache_path(display_key)
        meta_path = cache_path.with_name(f"{cache_path.name}.meta.json")
        lock_path = cache_path.with_name(f"{cache_path.name}.lock")

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        source_version = version_fn()
        reader_lease = self._acquire_read_lock(lock_path)
        try:
            cached_path = self._cached_parquet_path_if_valid(
                cache_path,
                meta_path,
                display_key,
                source_version,
                source_sql,
            )
            if cached_path is not None:
                return export_fn(cached_path)
        finally:
            self._release_read_lock(reader_lease)

        writer_lease = self._acquire_write_lock(lock_path)
        try:
            # One source query does double duty here: it rechecks the entry
            # (another writer may have populated it while we waited for the write
            # lock) and serves as the "before" bracket for the first cold-load
            # attempt, rather than querying the source twice back-to-back.
            source_version = version_fn()
            cached_path = self._cached_parquet_path_if_valid(
                cache_path,
                meta_path,
                display_key,
                source_version,
                source_sql,
            )
            if cached_path is not None:
                return export_fn(cached_path)

            # The version is snapshotted once, before the write, and stored as
            # the entry's version. A streaming source that gives statement-level
            # read consistency (Oracle) produces a snapshot that cannot be torn,
            # so there is no need to bracket the write with a second read. If the
            # table did change between this read and the write's snapshot, the
            # stored version is merely older than the data, so the next reader
            # recomputes a newer version, misses, and reloads exactly once --
            # never serving stale. This also sidesteps MAX(ORA_ROWSCN) drift from
            # delayed block cleanout, which would otherwise make a "before vs.
            # after" bracket disagree forever on a freshly loaded table.
            part_path = self._part_path(cache_path)
            meta_part_path = self._part_path(meta_path)
            try:
                write_fn(part_path)
                self._write_parquet_file_entry(
                    part_path,
                    cache_path,
                    meta_part_path,
                    meta_path,
                    display_key,
                    source_version,
                    source_sql,
                )
                return export_fn(cache_path)
            except Exception:
                self._remove_file(part_path)
                self._remove_file(meta_part_path)
                raise
        finally:
            self._release_write_lock(writer_lease)

    def _acquire_deadline(self) -> float | None:
        if self.acquire_timeout_seconds is None:
            return None
        return time.monotonic() + self.acquire_timeout_seconds

    def _poll_backoff(self, deadline: float | None, what: str) -> None:
        # One retry tick. Raises once the optional acquisition deadline passes so
        # a wedged share cannot spin a client forever.
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(
                f"timed out after {self.acquire_timeout_seconds}s acquiring {what}"
            )
        time.sleep(self.poll_seconds)

    def _acquire_read_lock(self, lock_path: Path) -> _LockLease:
        readers_path = lock_path / "readers"
        writer_path = lock_path / "writer"
        # Stable token name for this whole acquisition. Generating a fresh UUID
        # on every retry mints a new directory each spin, which leaks reader
        # folders on shares where removal is racy (e.g. Windows/SMB).
        reader_path = readers_path / self._reader_lock_name()
        deadline = self._acquire_deadline()

        while True:
            if not self._ensure_lock_dirs(lock_path):
                self._poll_backoff(deadline, "read lock")
                continue
            if writer_path.exists():
                self._break_stale_lock(writer_path)
                self._poll_backoff(deadline, "read lock")
                continue

            try:
                reader_path.mkdir()
            except FileExistsError:
                # Our own token survived a previous iteration (a release that
                # lost the rmdir race). Clear it and retry rather than spin.
                self._remove_lock_dir(reader_path)
                self._poll_backoff(deadline, "read lock")
                continue
            except FileNotFoundError:
                self._poll_backoff(deadline, "read lock")
                continue
            except OSError as exc:
                if self._is_transient_lock_mkdir_error(exc):
                    self._poll_backoff(deadline, "read lock")
                    continue
                raise

            try:
                reader_lease = self._start_lock_heartbeat(
                    reader_path,
                    "reader",
                    create_missing=True,
                )
            except OSError as exc:
                self._remove_lock_dir(reader_path)
                if not self._is_transient_lock_mkdir_error(exc):
                    raise
                self._poll_backoff(deadline, "read lock")
                continue

            if not writer_path.exists():
                return reader_lease

            self._release_read_lock(reader_lease)
            self._poll_backoff(deadline, "read lock")

    def _acquire_write_lock(self, lock_path: Path) -> _LockLease:
        readers_path = lock_path / "readers"
        writer_path = lock_path / "writer"
        deadline = self._acquire_deadline()

        while True:
            if not self._ensure_lock_dirs(lock_path):
                self._poll_backoff(deadline, "write lock")
                continue
            try:
                writer_path.mkdir()
                writer_lease = self._start_lock_heartbeat(writer_path, "writer")
                break
            except FileExistsError:
                self._break_stale_lock(writer_path)
                self._poll_backoff(deadline, "write lock")
            except FileNotFoundError:
                self._poll_backoff(deadline, "write lock")
                continue
            except OSError as exc:
                if self._is_transient_lock_mkdir_error(exc):
                    self._poll_backoff(deadline, "write lock")
                    continue
                raise

        try:
            while self._has_readers(readers_path):
                self._poll_backoff(deadline, "write lock (draining readers)")
            return writer_lease
        except Exception:
            self._release_write_lock(writer_lease)
            raise

    def _release_read_lock(self, lease: _LockLease | Path) -> None:
        reader_path = self._lease_path(lease)
        self._stop_lock_heartbeat(lease)
        lock_path = reader_path.parent.parent
        self._remove_lock_dir_resilient(reader_path)
        self._cleanup_lock_dirs(lock_path)

    def _release_write_lock(self, lease: _LockLease | Path) -> None:
        writer_path = self._lease_path(lease)
        self._stop_lock_heartbeat(lease)
        lock_path = writer_path.parent
        self._remove_lock_dir_resilient(writer_path)
        self._cleanup_lock_dirs(lock_path)

    def _remove_lock_dir_resilient(self, lock_path: Path) -> None:
        # A heartbeat thread that was mid-write when we asked it to stop can
        # re-create lock.json after _remove_lock_dir deletes it, leaving a
        # non-empty dir that rmdir then skips. Retry until the dir is gone so a
        # stopped lock never lingers and blocks other clients (Windows/SMB).
        for _ in range(5):
            self._remove_lock_dir(lock_path)
            if not lock_path.exists():
                return
            time.sleep(self.poll_seconds)
        self._remove_lock_dir(lock_path)

    @staticmethod
    def _lease_path(lease: _LockLease | Path) -> Path:
        return lease.path if isinstance(lease, _LockLease) else lease

    @staticmethod
    def _stop_lock_heartbeat(lease: _LockLease | Path) -> None:
        if not isinstance(lease, _LockLease):
            return

        lease.stop_event.set()
        # Join long enough to cover a heartbeat blocked on a slow share write,
        # so the thread cannot re-create lock.json after we remove the dir.
        lease.thread.join(timeout=5.0)

    def _ensure_lock_dirs(self, lock_path: Path) -> bool:
        # Returns True only when readers/ is confirmed to exist. A transient
        # mkdir failure (common on NFS/SMB) returns False so the caller retries
        # rather than racing into a child mkdir that fails with FileNotFoundError.
        readers_path = lock_path / "readers"
        try:
            readers_path.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            return True
        except OSError as exc:
            if not self._is_transient_lock_mkdir_error(exc):
                raise
            return readers_path.exists()
        return True

    def _has_readers(self, readers_path: Path) -> bool:
        has_reader = False
        try:
            reader_paths = list(readers_path.iterdir())
        except FileNotFoundError:
            return False

        for reader_path in reader_paths:
            if ".steal." in reader_path.name:
                # Transient artifact of a reader-token steal in progress; never a
                # live reader. Clean it only if its stealer died and left it
                # stale, otherwise let the stealer finish.
                if self._is_stale_lock(reader_path):
                    self._remove_lock_dir(reader_path)
                continue
            if not reader_path.is_dir():
                self._remove_file(reader_path)
                continue
            if self._break_stale_lock(reader_path):
                continue
            has_reader = True

        return has_reader

    @staticmethod
    def _cleanup_lock_dirs(lock_path: Path) -> None:
        # Keep shared coordination directories in place. Removing them after a
        # release races with other clients creating reader tokens on NFS.
        _ = lock_path

    @staticmethod
    def _is_transient_lock_mkdir_error(exc: OSError) -> bool:
        return exc.errno in {errno.ENOENT, errno.EINVAL}

    @staticmethod
    def _reader_lock_name() -> str:
        hostname = socket.gethostname().replace("/", "_")[:16]
        return f"r.{hostname}.{os.getpid()}.{uuid.uuid4().hex[:16]}"

    def _start_lock_heartbeat(
        self,
        lock_path: Path,
        lock_type: str,
        *,
        create_missing: bool = False,
    ) -> _LockLease:
        lock_uuid = uuid.uuid4().hex
        created_at = self._utc_now()
        self._write_lock_metadata(
            lock_path,
            lock_type,
            lock_uuid,
            created_at,
            create_missing=create_missing,
        )
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._heartbeat_lock,
            args=(lock_path, lock_type, lock_uuid, created_at, stop_event),
            daemon=True,
        )
        thread.start()
        return _LockLease(lock_path, stop_event, thread)

    def _heartbeat_lock(
        self,
        lock_path: Path,
        lock_type: str,
        lock_uuid: str,
        created_at: str,
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.wait(self.heartbeat_seconds):
            if not lock_path.exists():
                return
            try:
                self._write_lock_metadata(lock_path, lock_type, lock_uuid, created_at)
            except OSError:
                return

    def _write_lock_metadata(
        self,
        lock_path: Path,
        lock_type: str,
        lock_uuid: str,
        created_at: str,
        *,
        create_missing: bool = False,
    ) -> None:
        if create_missing:
            lock_path.mkdir(parents=True, exist_ok=True)

        metadata_path = lock_path / "lock.json"
        part_path = metadata_path.with_name(
            f"l.{os.getpid()}.{uuid.uuid4().hex[:16]}.tmp"
        )
        metadata = {
            "metadata_version": LOCK_METADATA_VERSION,
            "lock_type": lock_type,
            "hostname": self._hostname,
            "pid": os.getpid(),
            "uuid": lock_uuid,
            "created_at": created_at,
            "last_seen": self._utc_now(),
        }
        before = time.time()
        part_path.write_text(
            json.dumps(metadata, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(part_path, metadata_path)
        # Learn our offset from the server clock for free: os.replace preserves
        # the part file's server-stamped mtime, so the committed lock.json mtime
        # is "now" in the server's clock domain.
        try:
            self._record_clock_offset(
                metadata_path.stat().st_mtime, before, time.time()
            )
        except OSError:
            pass

    def _break_stale_lock(self, lock_path: Path) -> bool:
        # Steal a stale lock by atomic rename, not blind removal. Renaming the
        # entry to a unique private name is the only compare-and-swap a shared
        # filesystem offers: of many clients racing the same stale lock, exactly
        # one rename of a given directory succeeds, so a second breaker can never
        # delete a lock a third client legitimately recreated (the cascade the
        # old check-then-rmdir allowed).
        if not self._is_stale_lock(lock_path):
            return False

        steal_path = lock_path.with_name(
            f"{lock_path.name}.steal.{os.getpid()}.{uuid.uuid4().hex[:16]}"
        )
        try:
            os.rename(lock_path, steal_path)
        except OSError as exc:
            # ENOENT/ESTALE: another breaker won, or the owner released it.
            # ENOTDIR/EINVAL: the entry changed shape under us. All mean retry.
            if exc.errno in {
                errno.ENOENT,
                errno.ESTALE,
                errno.ENOTDIR,
                errno.EINVAL,
            }:
                return False
            raise

        # We now exclusively own steal_path. Re-check the entry we actually
        # grabbed: a lock recreated between the staleness check above and the
        # rename would otherwise be destroyed, so if it is no longer stale we put
        # it back untouched.
        if not self._is_stale_lock(steal_path):
            self._restore_stolen_lock(steal_path, lock_path)
            return False

        logger.info(f"Breaking stale cache lock: {lock_path}")
        self._remove_lock_dir(steal_path)
        return True

    def _restore_stolen_lock(self, steal_path: Path, lock_path: Path) -> None:
        # The entry was live after all (recreated during the steal). Put it back
        # if its slot is still free; if a new owner already claimed the slot,
        # drop our moved copy rather than overwrite the new owner or leak it.
        try:
            os.rename(steal_path, lock_path)
        except OSError:
            self._remove_lock_dir(steal_path)

    def _is_stale_lock(self, lock_path: Path) -> bool:
        # A lock held by a dead process on this host is reclaimable at once,
        # without waiting out the mtime timeout — its pid no longer exists.
        if self._lock_owner_is_dead(lock_path):
            return True

        try:
            lock_mtime = (lock_path / "lock.json").stat().st_mtime
        except FileNotFoundError:
            try:
                lock_mtime = lock_path.stat().st_mtime
            except FileNotFoundError:
                return False

        # Cold start: if we have never measured our offset from the server clock,
        # do it once so the comparison below is in the server's domain. (A client
        # whose clock runs behind would otherwise read every lock as fresh and
        # never break a genuinely dead one.)
        if self._clock_offset_at is None:
            self._refresh_clock_offset()

        if not self._exceeds_stale_age(lock_mtime):
            return False

        # It looks stale, but a drifted offset could be lying. Re-measure against
        # a fresh server timestamp before committing to a break, unless we just
        # measured it.
        if not self._clock_offset_is_fresh():
            self._refresh_clock_offset()
            return self._exceeds_stale_age(lock_mtime)

        return True

    def _lock_owner_is_dead(self, lock_path: Path) -> bool:
        # A pid is meaningful only on the host that minted it, so only same-host
        # owners are checked. A missing/foreign/unreadable owner falls through to
        # the mtime-based staleness test.
        try:
            metadata = json.loads(
                (lock_path / "lock.json").read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            return False
        if not isinstance(metadata, dict):
            return False
        if metadata.get("hostname") != self._hostname:
            return False
        pid = metadata.get("pid")
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            return False
        return self._pid_is_dead(pid)

    @staticmethod
    def _pid_is_dead(pid: int) -> bool:
        # Only declare death on a definitive "no such process". Any other error
        # (e.g. EPERM: exists but owned by another user) means assume alive, so a
        # live lock is never broken on uncertainty.
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except OSError:
            return False
        return False

    def _exceeds_stale_age(self, lock_mtime: float) -> bool:
        server_now = time.time() + self._clock_offset
        return (server_now - lock_mtime) > self.stale_lock_seconds

    def _clock_offset_is_fresh(self) -> bool:
        if self._clock_offset_at is None:
            return False
        age = time.monotonic() - self._clock_offset_at
        return age <= self._clock_probe_interval

    def _record_clock_offset(
        self, server_mtime: float, before: float, after: float
    ) -> None:
        # The midpoint of the local interval bracketing the write best estimates
        # the local instant the server stamped the mtime; their difference is our
        # offset from the server clock.
        self._clock_offset = server_mtime - (before + after) / 2.0
        self._clock_offset_at = time.monotonic()

    def _refresh_clock_offset(self) -> None:
        probe_path = self.cache_dir / (
            f".clock.{os.getpid()}.{uuid.uuid4().hex[:16]}.tmp"
        )
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            before = time.time()
            probe_path.write_text("", encoding="utf-8")
            server_mtime = probe_path.stat().st_mtime
            after = time.time()
        except OSError:
            return
        finally:
            self._remove_file(probe_path)
        self._record_clock_offset(server_mtime, before, after)

    @staticmethod
    def _remove_lock_dir(lock_path: Path) -> None:
        try:
            children = list(lock_path.iterdir())
        except FileNotFoundError:
            return
        except NotADirectoryError:
            try:
                lock_path.unlink()
            except (FileNotFoundError, OSError):
                pass
            return

        for child in children:
            try:
                if child.is_dir() and not child.is_symlink():
                    child.rmdir()
                else:
                    child.unlink()
            except (FileNotFoundError, OSError):
                pass

        try:
            lock_path.rmdir()
        except (FileNotFoundError, OSError):
            pass

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(UTC).isoformat()

    def _cached_parquet_path_if_valid(
        self,
        cache_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> Path | None:
        metadata = self._read_valid_metadata(
            cache_path,
            meta_path,
            display_key,
            source_version,
            source_sql,
        )
        if metadata is None:
            return None

        version_preview = self._version_preview(metadata.get("source_version"))
        logger.info(
            f"Returning cached object: {display_key}{version_preview}..."
        )
        return cache_path

    def _write_parquet_file_entry(
        self,
        part_path: Path,
        cache_path: Path,
        meta_part_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> None:
        try:
            parquet_metadata = pq.read_metadata(str(part_path))
            schema = pq.read_schema(str(part_path))
            self._write_metadata(
                meta_part_path,
                display_key=display_key,
                cache_path=cache_path,
                parquet_path=part_path,
                source_version=source_version,
                source_sql=source_sql,
                row_count=parquet_metadata.num_rows,
                column_count=len(schema),
                schema=schema,
            )
            os.replace(part_path, cache_path)
            os.replace(meta_part_path, meta_path)
        except Exception:
            self._remove_file(part_path)
            self._remove_file(meta_part_path)
            raise

    def _copy_cached_parquet(self, cache_path: Path, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_part = self._part_path(output_path)
        try:
            shutil.copyfile(cache_path, output_part)
            os.replace(output_part, output_path)
        except Exception:
            self._remove_file(output_part)
            raise

    def _write_metadata(
        self,
        meta_part_path: Path,
        *,
        display_key: str,
        cache_path: Path,
        parquet_path: Path,
        source_version: str | None,
        source_sql: str | None,
        row_count: int,
        column_count: int,
        schema: object,
    ) -> None:
        schema_metadata = self._schema_metadata(schema)
        metadata = {
            "metadata_version": CACHE_METADATA_VERSION,
            "writer_version": CACHE_WRITER_VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "source_key": display_key,
            "source_version": source_version,
            "parquet": {
                "file_name": cache_path.name,
                "size_bytes": parquet_path.stat().st_size,
                "sha256": self._file_hash(parquet_path),
            },
            "data": {
                "row_count": row_count,
                "column_count": column_count,
                "schema_hash": self._schema_hash(schema_metadata),
            },
        }
        if source_sql is not None:
            metadata["source_sql"] = source_sql
        meta_part_path.write_text(
            json.dumps(metadata, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _file_hash(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(8 * 1024 * 1024), b""):
                digest.update(chunk)

        return digest.hexdigest()

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        return " ".join(sql.split()).strip().rstrip(";")

    # Parameter names the SQL/path arguments may appear under. The decorated
    # function's signature is matched against these so call sites can name the
    # arguments naturally (e.g. `sql_query`, `parquet_path`).
    _SQL_ARG_NAMES = ("sql", "sql_query")
    _PATH_ARG_NAMES = ("parquet_path", "output_path")

    @staticmethod
    def _sql_arg(
        signature: inspect.Signature,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> object:
        for name in NFSCache._SQL_ARG_NAMES:
            if name in kwargs:
                return kwargs[name]

        try:
            bound = signature.bind_partial(*args, **kwargs)
        except TypeError:
            if args:
                return args[0]
            raise

        for name in NFSCache._SQL_ARG_NAMES:
            if name in bound.arguments:
                return bound.arguments[name]

        if args:
            return args[0]

        raise TypeError("sql requires a sql argument")

    @staticmethod
    def _path_arg(
        signature: inspect.Signature,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> object:
        bound = signature.bind_partial(*args, **kwargs)
        for name in NFSCache._PATH_ARG_NAMES:
            if name in bound.arguments:
                return bound.arguments[name]

        raise TypeError("sql_parquet requires an output path argument")

    @staticmethod
    def _replace_path_arg(
        signature: inspect.Signature,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
        path: Path,
    ) -> tuple[tuple[object, ...], dict[str, object]]:
        bound = signature.bind_partial(*args, **kwargs)
        for name in NFSCache._PATH_ARG_NAMES:
            if name in bound.arguments:
                bound.arguments[name] = path
                return bound.args, bound.kwargs

        raise TypeError("sql_parquet requires an output path argument")

    def _table_from_sql(self, sql: str) -> str | None:
        match = self._FROM_RE.search(sql)
        if match is None:
            return None
        # Strip quoting; keep schema.table as-is for the version query.
        return match.group(1).strip('"')

    def _sql_display_key(
        self,
        sql: str,
        return_cols: object | None = None,
    ) -> str:
        normalized_sql = self._normalize_sql(sql)
        if return_cols:
            cols_key = "-".join(sorted(str(col).upper() for col in return_cols))
        else:
            cols_key = "-"
        table_name = (self._table_from_sql(normalized_sql) or "NO_TABLE").upper()
        fingerprint = hashlib.sha256(
            f"{normalized_sql}|{cols_key}".encode("utf-8")
        ).hexdigest()[:SQL_CACHE_FINGERPRINT_HEX_LENGTH]
        return f"sql/{table_name}/{fingerprint}.parquet"

    def _sql_source_version(self, sql: str) -> str | None:
        if self.connect_factory is None:
            return None

        table_name = self._table_from_sql(self._normalize_sql(sql))
        if table_name is None:
            return None

        version_query = self.VERSION_SQL.format(table=table_name)
        with self.connect_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(version_query)
                row = cursor.fetchone()

        n_rows = row[0] if row else 0
        scn = int(row[1]) if row and row[1] is not None else 0
        return f"{table_name.upper()}@SCN:{scn}|ROWS:{n_rows}"

    def _read_valid_metadata(
        self,
        cache_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> dict[str, object] | None:
        if not self._is_complete(cache_path):
            return None

        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            logger.info(
                f"Ignoring incomplete cache entry: {display_key}: "
                "missing metadata"
            )
            return None
        except json.JSONDecodeError as exc:
            logger.info(
                f"Ignoring corrupt cache entry: {display_key}: "
                f"metadata is not valid JSON: {exc}"
            )
            return None

        reason = self._metadata_reject_reason(
            metadata,
            cache_path,
            display_key,
            source_version,
            source_sql,
        )
        if reason is not None:
            logger.info(f"Ignoring cache entry: {display_key}: {reason}")
            return None

        return metadata

    def _metadata_reject_reason(
        self,
        metadata: object,
        cache_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> str | None:
        if not isinstance(metadata, dict):
            return "corrupt metadata: top-level JSON must be an object"

        if metadata.get("metadata_version") != CACHE_METADATA_VERSION:
            return "unsupported metadata version"

        if metadata.get("writer_version") != CACHE_WRITER_VERSION:
            return "unsupported writer version"

        if metadata.get("source_key") != display_key:
            return "source key mismatch"

        if source_sql is not None and metadata.get("source_sql") != source_sql:
            return "source SQL mismatch"

        if "source_version" not in metadata:
            return "corrupt metadata: missing source version"

        if metadata.get("source_version") != source_version:
            return "stale source version"

        parquet_meta = metadata.get("parquet")
        if not isinstance(parquet_meta, dict):
            return "corrupt metadata: missing parquet section"

        if parquet_meta.get("file_name") != cache_path.name:
            return "parquet file name mismatch"

        try:
            stat = cache_path.stat()
        except FileNotFoundError:
            return "incomplete entry: missing parquet"

        if stat.st_size <= 0:
            return "incomplete entry: empty parquet"

        if parquet_meta.get("size_bytes") != stat.st_size:
            return "parquet size mismatch"

        expected_sha = parquet_meta.get("sha256")
        if not isinstance(expected_sha, str) or len(expected_sha) != 64:
            return "corrupt metadata: invalid parquet checksum"

        # Full-file hashing is opt-in (see verify_checksum): it re-reads the
        # entire parquet on every warm hit. The size check above plus the
        # parquet-footer checks below already pin the committed file's identity;
        # the checksum only adds silent-corruption detection.
        if self.verify_checksum:
            try:
                actual_sha = self._file_hash(cache_path)
            except OSError as exc:
                return f"cannot read parquet bytes: {exc}"

            if actual_sha != expected_sha:
                return "parquet checksum mismatch"

        data_meta = metadata.get("data")
        if not isinstance(data_meta, dict):
            return "corrupt metadata: missing data section"

        try:
            parquet_file = pq.ParquetFile(str(cache_path))
        except Exception as exc:
            return f"corrupt parquet: unreadable metadata: {exc}"

        row_count = data_meta.get("row_count")
        if row_count != parquet_file.metadata.num_rows:
            return "parquet row count mismatch"

        schema_metadata = self._schema_metadata(parquet_file.schema_arrow)
        if data_meta.get("column_count") != len(schema_metadata):
            return "parquet column count mismatch"

        if data_meta.get("schema_hash") != self._schema_hash(schema_metadata):
            return "parquet schema hash mismatch"

        return None

    @staticmethod
    def _version_preview(source_version: str | None) -> str:
        if source_version is None:
            return ""

        return f" version={source_version[:40]}"

    def _cache_path(self, display_key: str) -> Path:
        key_path = Path(display_key)
        if key_path.is_absolute():
            digest = hashlib.sha256(display_key.encode("utf-8")).hexdigest()
            return self.cache_dir / "_absolute" / digest / key_path.name

        if ".." in key_path.parts:
            digest = hashlib.sha256(display_key.encode("utf-8")).hexdigest()
            suffix = key_path.suffix or ".parquet"
            return self.cache_dir / "_relative" / f"{digest}{suffix}"

        return self.cache_dir.joinpath(*key_path.parts)

    @staticmethod
    def _part_path(path: Path) -> Path:
        return path.with_name(
            f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.part"
        )

    @staticmethod
    def _schema_metadata(schema: object) -> list[dict[str, object]]:
        return [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": bool(field.nullable),
            }
            for field in schema
        ]

    @staticmethod
    def _schema_hash(schema_metadata: list[dict[str, object]]) -> str:
        encoded = json.dumps(
            schema_metadata,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _is_complete(path: Path) -> bool:
        try:
            return path.stat().st_size > 0
        except FileNotFoundError:
            return False

    @staticmethod
    def _remove_file(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
