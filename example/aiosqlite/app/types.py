"""Hand-written types referenced from butter.toml `column-types` overrides."""

import datetime
import enum
import typing

import pydantic


class Status(enum.StrEnum):
    open = "OPEN"
    pending = "PENDING"
    active = "ACTIVE"
    archived = "ARCHIVED"


_SQLITE_TIMESTAMP = "%Y-%m-%d %H:%M:%S"


def _parse_sql_utc_timestamp(value: object) -> object:
    if isinstance(value, str):
        parsed = datetime.datetime.fromisoformat(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=datetime.UTC)
    if isinstance(value, datetime.datetime) and value.tzinfo is None:
        return value.replace(tzinfo=datetime.UTC)
    return value


def _serialize_sql_utc_timestamp(value: datetime.datetime) -> str:
    """Matches SQLite's own `datetime('now')` text so string comparisons sort correctly."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.UTC)
    return value.astimezone(datetime.UTC).strftime(_SQLITE_TIMESTAMP)


SqlUtcTimestamp = typing.Annotated[
    datetime.datetime,
    pydantic.BeforeValidator(_parse_sql_utc_timestamp),
    pydantic.PlainSerializer(_serialize_sql_utc_timestamp, return_type=str),
]
