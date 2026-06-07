from argparse import Namespace
from collections.abc import Callable, Mapping
from pathlib import Path

EnvField = tuple[str, Callable[[str], object]]

ORACLE_ENV_FIELDS: Mapping[str, EnvField] = {
    "ORACLE_HOST": ("host", str),
    "ORACLE_PORT": ("port", int),
    "ORACLE_SERVICE": ("service", str),
    "ORACLE_USER": ("user", str),
    "ORACLE_PASSWORD": ("password", str),
    "ORACLE_TABLE": ("table", str),
    "ORACLE_BATCH_SIZE": ("batch_size", int),
}


def find_dotenv() -> Path | None:
    for path in (Path.cwd(), *Path.cwd().parents):
        dotenv_path = path / ".env"
        try:
            if dotenv_path.is_file():
                return dotenv_path
        except OSError:
            # Unreadable .env (e.g. restricted perms): treat as absent.
            continue
    return None


def read_dotenv() -> dict[str, str]:
    dotenv_path = find_dotenv()
    if dotenv_path is None:
        return {}

    try:
        text = dotenv_path.read_text(encoding="utf-8")
    except OSError:
        # Fall back to defaults rather than crashing on an unreadable .env.
        return {}

    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped.removeprefix("export ").strip()
        if "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in {"'", '"'}
        ):
            value = value[1:-1]
        values[key] = value

    return values


def apply_dotenv(
    args: Namespace,
    *,
    fields: Mapping[str, EnvField] = ORACLE_ENV_FIELDS,
) -> None:
    values = read_dotenv()
    for env_key, (arg_name, coerce) in fields.items():
        if env_key not in values:
            continue
        value = values[env_key]
        if value == "":
            continue
        setattr(args, arg_name, coerce(value))
