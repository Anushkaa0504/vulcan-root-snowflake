from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone

from vulcan import DatetimeRanges, signal


@signal()
def stabilized_intervals(batch: DatetimeRanges, days: int = 1) -> t.Union[bool, DatetimeRanges]:
    """Allow only intervals that ended at least `days` ago."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    return [
        (start, end)
        for start, end in batch
        if end <= cutoff
    ]
