"""
Smoke test for the generated SQLite query module.

Creates an in-memory SQLite database, runs the schema DDL, then exercises
the generated functions from `generated/main.py`.

Usage:
    python example/sqlite/smoke_test.py
"""

import os
import sqlite3
import sys

# Allow importing the generated module relative to this file's location
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated"))

import main as queries  # noqa: E402  (generated module)


def load_schema(conn: sqlite3.Connection) -> None:
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        sql = f.read()

    # SQLite does not support CREATE TEMPORARY TABLE inside executescript when
    # there is an active transaction, so we filter out TEMPORARY tables and
    # objects that are only needed for the real database (indexes, etc.) that
    # may reference tables not relevant to our smoke test.  We run each
    # statement individually so we can skip known-benign ones gracefully.
    _BENIGN_PATTERNS = (
        "temporary",  # CREATE TEMPORARY TABLE — not supported in executescript
        "expression",  # expression indexes on functions unsupported by older SQLite
        "no such function",  # sqlite functions unavailable in the current build
    )
    skipped: list[tuple[str, str]] = []
    for statement in sql.split(";"):
        stmt = statement.strip()
        if not stmt:
            continue
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if any(pat in msg for pat in _BENIGN_PATTERNS):
                skipped.append((stmt, str(exc)))
            else:
                raise

    conn.commit()

    users_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='users'"
    ).fetchone()
    assert users_exists is not None, (
        "Schema load failed: required table `users` not found"
    )

    if skipped:
        print(
            f"  (skipped {len(skipped)} schema statement(s) due to known SQLite limitations)"
        )


def test_create_and_get_user(cursor: sqlite3.Cursor) -> None:
    # Create a user
    queries.create_user(cursor, email="alice@example.com")

    # Fetch the user back
    cursor.execute("SELECT id FROM users WHERE email = 'alice@example.com'")
    row = cursor.fetchone()
    assert row is not None, "Expected to find alice@example.com in users table"
    user_id: int = row[0]

    result = queries.get_user_by_id(cursor, id=user_id)
    assert result is not None, "get_user_by_id returned None for a known user"
    assert result.email == "alice@example.com", (
        f"Expected email 'alice@example.com', got '{result.email}'"
    )
    assert result.user_id == user_id, (
        f"Expected user_id {user_id}, got {result.user_id}"
    )
    print(f"  ✓ create_user / get_user_by_id: {result}")


def test_get_user_by_id_missing(cursor: sqlite3.Cursor) -> None:
    result = queries.get_user_by_id(cursor, id=999999)
    assert result is None, "Expected None for a non-existent user id"
    print("  ✓ get_user_by_id (missing): returned None as expected")


def test_update_user_email(cursor: sqlite3.Cursor) -> None:
    queries.create_user(cursor, email="bob@example.com")
    cursor.execute("SELECT id FROM users WHERE email = 'bob@example.com'")
    row = cursor.fetchone()
    assert row is not None
    user_id: int = row[0]

    queries.update_user_email(cursor, email="bob-updated@example.com", id=user_id)

    result = queries.get_user_by_id(cursor, id=user_id)
    assert result is not None
    assert result.email == "bob-updated@example.com", (
        f"Expected updated email, got '{result.email}'"
    )
    print(f"  ✓ update_user_email: {result}")


def test_delete_user(cursor: sqlite3.Cursor) -> None:
    queries.create_user(cursor, email="charlie@example.com")
    cursor.execute("SELECT id FROM users WHERE email = 'charlie@example.com'")
    row = cursor.fetchone()
    assert row is not None
    user_id: int = row[0]

    queries.delete_user(cursor, id=user_id)

    result = queries.get_user_by_id(cursor, id=user_id)
    assert result is None, "Expected None after deleting the user"
    print("  ✓ delete_user: user no longer found after deletion")


def main() -> None:
    conn = sqlite3.connect(":memory:")
    load_schema(conn)
    cursor = conn.cursor()

    print("Running smoke tests …")
    test_create_and_get_user(cursor)
    test_get_user_by_id_missing(cursor)
    test_update_user_email(cursor)
    test_delete_user(cursor)

    conn.close()
    print("\nAll smoke tests passed ✓")


if __name__ == "__main__":
    main()
