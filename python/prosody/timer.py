from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Timer:
    """
    Represents a timer event with associated metadata.

    This class encapsulates the core components of a timer event, including
    its key, scheduled time, and any associated data.
    """

    key: str
    """The entity key that this timer belongs to."""

    time: datetime
    """The scheduled execution time of the timer."""