"""
Smoke test for the generated SQLite query module.

Creates an in-memory SQLite database, runs the schema DDL, then exercises
the generated functions from `generated/main.py`.

Usage:
    cd example/sqlite
    butter generate
    uv run python smoke_test.py
"""

import asyncio
import pathlib
import sys

import aiosqlite

HERE = pathlib.Path(__file__).parent

# Allow importing the generated module relative to this file's location.
sys.path.insert(0, str(HERE / "generated"))

import main as queries  # noqa: E402  (generated module)

# Statements that some SQLite builds reject; skipping them keeps the smoke
# test focused on the tables the queries use.
_BENIGN_PATTERNS = ("temporary", "expression", "no such function")


async def load_schema(connection: aiosqlite.Connection) -> None:
    sql = (HERE / "schema.sql").read_text()
    skipped = 0
    for statement in sql.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        try:
            await connection.execute(statement)
        except aiosqlite.OperationalError as exc:
            if any(pattern in str(exc).lower() for pattern in _BENIGN_PATTERNS):
                skipped += 1
            else:
                raise
    await connection.commit()

    async with connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='users'") as cursor:
        assert await cursor.fetchone() is not None, "Schema load failed: required table `users` not found"

    if skipped:
        print(f"  (skipped {skipped} schema statement(s) due to known SQLite limitations)")


async def test_create_and_get_user(cursor: aiosqlite.Cursor) -> None:
    user_id = await queries.create_user(cursor, email="alice@example.com")
    assert user_id is not None

    result = await queries.get_user_by_id(cursor, id=user_id)
    assert result is not None, "get_user_by_id returned None for a known user"
    assert result.email == "alice@example.com"
    assert result.user_id == user_id
    assert isinstance(result.created_at, str)
    print(f"  ✓ create_user / get_user_by_id: {result}")


async def test_get_user_by_id_missing(cursor: aiosqlite.Cursor) -> None:
    assert await queries.get_user_by_id(cursor, id=999999) is None
    print("  ✓ get_user_by_id (missing): returned None as expected")


async def test_update_user_email(cursor: aiosqlite.Cursor) -> None:
    user_id = await queries.create_user(cursor, email="bob@example.com")
    assert user_id is not None
    assert await queries.update_user_email(cursor, email="bob-updated@example.com", id=user_id) == 1

    result = await queries.get_user_by_id(cursor, id=user_id)
    assert result is not None and result.email == "bob-updated@example.com"
    print(f"  ✓ update_user_email: {result}")


async def test_delete_user(cursor: aiosqlite.Cursor) -> None:
    user_id = await queries.create_user(cursor, email="charlie@example.com")
    assert user_id is not None
    assert await queries.delete_user(cursor, id=user_id) == 1
    assert await queries.get_user_by_id(cursor, id=user_id) is None
    print("  ✓ delete_user: user no longer found after deletion")


async def test_orders(cursor: aiosqlite.Cursor) -> None:
    order_id = await queries.create_order(cursor, customer_id=7, status="pending")
    assert order_id is not None
    assert await queries.create_order_item(cursor, order_id=order_id, product_id=1, quantity=2) == 1
    inserted = await queries.create_order_items(
        cursor,
        [
            queries.CreateOrderItemsParams(order_id=order_id, product_id=2, quantity=1),
            queries.CreateOrderItemsParams(order_id=order_id, product_id=3, quantity=5),
        ],
    )
    assert inserted == 2

    rows = await queries.get_orders_with_items(cursor, customer_id=7)
    assert sorted((r.product_id, r.quantity) for r in rows) == [(1, 2), (2, 1), (3, 5)]
    assert all(r.status == "pending" for r in rows)

    assert await queries.delete_order_item(cursor, order_id=order_id, product_id=2) == 1
    assert len(await queries.get_orders_with_items(cursor, customer_id=7)) == 2
    print(f"  ✓ create_order / create_order_item(s) / get_orders_with_items / delete_order_item")


async def test_accounts_and_rectangles(cursor: aiosqlite.Cursor) -> None:
    await cursor.execute("INSERT INTO accounts (account_id, balance, account_type) VALUES (1, 10.0, 'checking')")
    assert await queries.adjust_account_balance(cursor, amount=2.5, account_id=1) == 1
    balance = await queries.get_account_balance(cursor, account_id=1)
    assert balance is not None and balance.balance == 12.5

    await cursor.execute("INSERT INTO rectangles (id, width, height) VALUES (1, 2.0, 3.0), (2, 1.0, 1.0)")
    areas = await queries.rectangle_areas(cursor, min_area=2.0)
    assert [(r.id, r.area) for r in areas] == [(1, 6.0)]
    print("  ✓ adjust_account_balance / get_account_balance / rectangle_areas")


async def main() -> None:
    async with aiosqlite.connect(":memory:") as connection:
        await load_schema(connection)
        async with connection.cursor() as cursor:
            print("Running smoke tests …")
            await test_create_and_get_user(cursor)
            await test_get_user_by_id_missing(cursor)
            await test_update_user_email(cursor)
            await test_delete_user(cursor)
            await test_orders(cursor)
            await test_accounts_and_rectangles(cursor)

    print("\nAll smoke tests passed ✓")


if __name__ == "__main__":
    asyncio.run(main())
