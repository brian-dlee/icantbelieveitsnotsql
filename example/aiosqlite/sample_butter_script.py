"""Smoke test for the generated code: run with `uv run python sample_butter_script.py`."""

import asyncio

import aiosqlite

from app.butter.cache_entry import (
    InsertManyCacheEntryParams,
    create_table_cache_entry,
    insert_many_cache_entry,
    select_many_cache_entry_by_key,
)


async def main() -> None:
    async with aiosqlite.connect(":memory:") as connection, connection.cursor() as cursor:
        await create_table_cache_entry(cursor)
        await insert_many_cache_entry(
            cursor,
            [InsertManyCacheEntryParams(key="greeting", value=b'{"text": "hello"}')],
        )
        print(await select_many_cache_entry_by_key(cursor, key="greeting"))


if __name__ == "__main__":
    asyncio.run(main())
