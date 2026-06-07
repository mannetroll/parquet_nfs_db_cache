import argparse
import unittest
from unittest import mock

from nfscache.database import oracle_pool


class FakePool:
    def __init__(self) -> None:
        self.acquired = 0

    def acquire(self) -> str:
        self.acquired += 1
        return f"conn-{self.acquired}"


def _args(*, host: str = "h", user: str = "u") -> argparse.Namespace:
    return argparse.Namespace(
        host=host,
        port=1521,
        service="FREEPDB1",
        user=user,
        password="pw",
    )


class OraclePoolTests(unittest.TestCase):
    def setUp(self) -> None:
        # The module caches pools globally; isolate each test.
        oracle_pool._pools.clear()
        self.addCleanup(oracle_pool._pools.clear)

    def test_pool_created_once_and_reused(self) -> None:
        with mock.patch.object(
            oracle_pool.oracledb, "create_pool", return_value=FakePool()
        ) as create_pool:
            args = _args()
            pool_a = oracle_pool.get_pool(args)
            pool_b = oracle_pool.get_pool(args)

            self.assertIs(pool_a, pool_b)
            create_pool.assert_called_once()

    def test_factory_acquires_without_recreating_pool(self) -> None:
        fake = FakePool()
        with mock.patch.object(
            oracle_pool.oracledb, "create_pool", return_value=fake
        ) as create_pool:
            factory = oracle_pool.make_pool_factory(_args())

            conn1 = factory()
            conn2 = factory()

            # Building the factory must not touch the DB; only calling it does.
            create_pool.assert_called_once()
            self.assertEqual((conn1, conn2), ("conn-1", "conn-2"))
            self.assertEqual(fake.acquired, 2)

    def test_factory_build_is_lazy(self) -> None:
        with mock.patch.object(
            oracle_pool.oracledb, "create_pool", return_value=FakePool()
        ) as create_pool:
            oracle_pool.make_pool_factory(_args())
            create_pool.assert_not_called()

    def test_distinct_dsn_or_user_get_distinct_pools(self) -> None:
        with mock.patch.object(
            oracle_pool.oracledb,
            "create_pool",
            side_effect=lambda **_: FakePool(),
        ) as create_pool:
            pool_a = oracle_pool.get_pool(_args(host="h1"))
            pool_b = oracle_pool.get_pool(_args(host="h2"))
            pool_c = oracle_pool.get_pool(_args(user="other"))

            self.assertIsNot(pool_a, pool_b)
            self.assertIsNot(pool_a, pool_c)
            self.assertEqual(create_pool.call_count, 3)

    def test_min_max_passed_through(self) -> None:
        with mock.patch.object(
            oracle_pool.oracledb, "create_pool", return_value=FakePool()
        ) as create_pool:
            oracle_pool.get_pool(_args(), min_size=2, max_size=9)

            _, kwargs = create_pool.call_args
            self.assertEqual(kwargs["min"], 2)
            self.assertEqual(kwargs["max"], 9)
            self.assertEqual(kwargs["dsn"], "h:1521/FREEPDB1")


if __name__ == "__main__":
    unittest.main()
