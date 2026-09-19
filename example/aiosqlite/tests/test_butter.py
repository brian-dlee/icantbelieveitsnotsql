"""Exercises every function butter generated for this sample project."""

import datetime

import aiosqlite
import pytest

from app.butter import cache_entry, membership, tag, ticket
from app.types import Status

UTC = datetime.UTC
NOW = datetime.datetime(2026, 9, 19, 12, 0, tzinfo=UTC)
OLD = NOW - datetime.timedelta(days=30)


@pytest.fixture
async def cursor():
    async with aiosqlite.connect(":memory:") as connection:
        # Positional row mapping must work with aiosqlite.Row as well as tuples.
        connection.row_factory = aiosqlite.Row
        async with connection.cursor() as cur:
            await cache_entry.create_table_cache_entry(cur)
            await tag.create_table_tag(cur)
            await ticket.create_table_ticket(cur)
            await membership.create_table_membership(cur)
            yield cur


def make_ticket(reference: str, status: Status, seen: datetime.datetime | None = NOW, active=None):
    return ticket.InsertManyTicketParams(
        reference=reference,
        label="import",
        reason="batch",
        last_seen_at=seen,
        active_since=active,
        status=status,
    )


# ------------------------------------------------------------- cache_entry


async def test_cache_entry_roundtrip(cursor):
    inserted = await cache_entry.insert_many_cache_entry(
        cursor,
        [
            cache_entry.InsertManyCacheEntryParams(key="a", value=b'{"n":1}'),
            cache_entry.InsertManyCacheEntryParams(key="b", value=b"\x00\xffbinary"),
        ],
    )
    assert inserted == 2

    rows = await cache_entry.select_many_cache_entry(cursor)
    assert {r.key for r in rows} == {"a", "b"}
    assert all(isinstance(r.value, bytes) for r in rows)

    by_key = await cache_entry.select_many_cache_entry_by_key(cursor, key="a")
    assert by_key == [cache_entry.SelectManyCacheEntryByKeyRow(key="a", value=b'{"n":1}')]

    one = await cache_entry.select_one_cache_entry_by_key(cursor, key="b")
    assert one is not None and one.value == b"\x00\xffbinary"
    assert await cache_entry.select_one_cache_entry_by_key(cursor, key="zzz") is None

    count = await cache_entry.count_cache_entry(cursor)
    assert count is not None and count.total == 2


async def test_cache_entry_works_without_row_factory():
    async with aiosqlite.connect(":memory:") as connection, connection.cursor() as cur:
        await cache_entry.create_table_cache_entry(cur)
        await cache_entry.insert_many_cache_entry(cur, [cache_entry.InsertManyCacheEntryParams(key="a", value=b"x")])
        assert (await cache_entry.select_many_cache_entry(cur))[0].key == "a"


# --------------------------------------------------------------------- tag


async def test_tag_ignores_duplicates(cursor):
    rows = [
        tag.InsertManyTagParams(item_id="item-1", name="urgent"),
        tag.InsertManyTagParams(item_id="item-1", name="blue"),
    ]
    assert await tag.insert_many_tag(cursor, rows) == 2
    assert await tag.insert_many_tag(cursor, rows) == 0

    names = await tag.select_many_tag_name_by_item_id(cursor, item_id="item-1")
    assert [n.name for n in names] == ["blue", "urgent"]


# -------------------------------------------------------------- membership


async def test_membership_and_left_join(cursor):
    await membership.insert_many_membership(
        cursor,
        [
            membership.InsertManyMembershipParams(user_id="u1", group_id="g1"),
            membership.InsertManyMembershipParams(user_id="u1", group_id="g2"),
        ],
    )
    assert (
        await membership.insert_many_membership_ignore_conflicts(
            cursor, [membership.InsertManyMembershipIgnoreConflictsParams(user_id="u1", group_id="g1")]
        )
        == 0
    )

    with pytest.raises(aiosqlite.IntegrityError):
        await membership.insert_many_membership(
            cursor, [membership.InsertManyMembershipParams(user_id="u1", group_id="g1")]
        )

    rows = await membership.select_many_membership_by_user_id(cursor, user_id="u1")
    assert sorted(r.group_id for r in rows) == ["g1", "g2"]

    await cache_entry.insert_many_cache_entry(cursor, [cache_entry.InsertManyCacheEntryParams(key="g1", value=b"info")])
    joined = await membership.select_many_membership_with_cache_entry(cursor, user_id="u1")
    by_group = {r.group_id: r.cached_value for r in joined}
    assert by_group == {"g1": b"info", "g2": None}


# ------------------------------------------------------------------ ticket


async def test_ticket_types_roundtrip(cursor):
    assert await ticket.insert_many_ticket(cursor, [make_ticket("T-1", Status.pending)]) == 1

    rows = await ticket.select_many_ticket(cursor)
    assert len(rows) == 1
    row = rows[0]
    assert row.status is Status.pending
    assert row.last_seen_at == NOW
    assert row.last_seen_at.tzinfo is not None
    assert row.active_since is None
    assert row.label == "import"

    # The timestamp is stored in SQLite's own text format so datetime() comparisons work.
    await cursor.execute("SELECT last_seen_at FROM ticket")
    assert (await cursor.fetchone())[0] == "2026-09-19 12:00:00"


async def test_ticket_filters(cursor):
    await ticket.insert_many_ticket(cursor, [make_ticket("T-1", Status.pending), make_ticket("T-2", Status.open)])

    pending = await ticket.select_many_ticket_by_status(cursor, status=Status.pending)
    assert [r.reference for r in pending] == ["T-1"]

    assert len(await ticket.select_many_ticket_filtered(cursor)) == 2
    assert len(await ticket.select_many_ticket_filtered(cursor, status=None)) == 2
    only_open = await ticket.select_many_ticket_filtered(cursor, status=Status.open)
    assert [r.reference for r in only_open] == ["T-2"]


async def test_ticket_conflicts(cursor):
    await ticket.insert_many_ticket(cursor, [make_ticket("T-1", Status.pending)])

    ignored = await ticket.insert_many_ticket_ignore_conflicts(
        cursor,
        [ticket.InsertManyTicketIgnoreConflictsParams(reference="T-1", status=Status.open)],
    )
    assert ignored == 0

    later = NOW + datetime.timedelta(hours=1)
    await ticket.upsert_many_ticket(
        cursor,
        [
            ticket.UpsertManyTicketParams(
                reference="T-1",
                label="manual",
                reason="review",
                last_seen_at=later,
                status=Status.open,  # not part of the DO UPDATE SET, must stay PENDING
            )
        ],
    )
    (row,) = await ticket.select_many_ticket(cursor)
    assert (row.label, row.reason, row.last_seen_at, row.status) == ("manual", "review", later, Status.pending)


async def test_ticket_lifecycle(cursor):
    await ticket.insert_many_ticket(
        cursor,
        [
            make_ticket("old", Status.active, seen=OLD, active=OLD),
            make_ticket("fresh", Status.pending, seen=NOW),
            make_ticket("fresher", Status.pending, seen=NOW + datetime.timedelta(minutes=5), active=NOW),
            make_ticket("stale-active", Status.open, seen=NOW - datetime.timedelta(hours=1), active=NOW),
        ],
    )

    assert await ticket.archive_stale_ticket(cursor, duration="-7 days") == 1
    (old,) = await ticket.select_many_ticket_by_status(cursor, status=Status.archived)
    assert old.reference == "old" and old.active_since is None

    # Two most recently seen non-archived rows: "fresher" (already started) and "fresh".
    assert await ticket.activate_recent_ticket(cursor, count=2) == 2
    by_ref = {r.reference: r for r in await ticket.select_many_ticket(cursor)}
    assert by_ref["fresh"].status is Status.pending
    assert by_ref["fresh"].active_since is not None
    assert by_ref["fresher"].status is Status.active

    # "stale-active" has an active timestamp but is neither ACTIVE nor PENDING.
    assert await ticket.archive_inactive_ticket(cursor) == 1

    counts = await ticket.count_ticket_by_status(cursor)
    assert {(c.status, c.total) for c in counts} == {
        (Status.archived, 2),
        (Status.pending, 1),
        (Status.active, 1),
    }
    assert all(isinstance(c.latest_seen, datetime.datetime) for c in counts)

    assert (
        await ticket.update_status_ticket_by_status(cursor, set_status=Status.active, where_status=Status.pending)
        == 1
    )
    assert await ticket.delete_ticket_by_status(cursor, where_status=Status.archived) == 2
    assert await ticket.update_status_ticket(cursor, set_status=Status.open) == 2
    assert {r.status for r in await ticket.select_many_ticket(cursor)} == {Status.open}
