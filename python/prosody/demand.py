"""Why a handler attempt runs: a normal delivery or a retry after a failure."""

import enum
from dataclasses import dataclass


class DemandKind(enum.Enum):
    """The reason for a handler attempt."""

    NORMAL = "normal"
    FAILURE = "failure"


@dataclass(frozen=True)
class Demand:
    """The demand that :attr:`Context.demand` reports for this attempt.

    ``retry`` is the retry ordinal: 0 for a normal delivery and 1 on the first
    retry after a failure. The ordinal restarts at 1 when Prosody defers an
    event after immediate retries, so it is an estimate. Keep an exact attempt
    count in keyed state if a handler needs one.
    """

    kind: DemandKind
    retry: int
