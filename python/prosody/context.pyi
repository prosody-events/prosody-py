from datetime import datetime
from typing import List

class Context:
    """
    Represents the context of a Kafka message.

    This class encapsulates contextual information about a Kafka message,
    which may be useful for message handling and processing.
    """
    
    async def schedule(self, time: datetime) -> None:
        """
        Schedule a new timer at the given execution time for the current message key.
        
        Args:
            time: A UTC datetime at which the timer should fire
        """
        ...
    
    async def clear_and_schedule(self, time: datetime) -> None:
        """
        Unschedule ALL existing timers for the current key, then schedule exactly one new timer.
        
        Args:
            time: The UTC time for the new, sole scheduled timer
        """
        ...
    
    async def unschedule(self, time: datetime) -> None:
        """
        Unschedule a specific timer for the current key at the specified time.
        
        Args:
            time: The UTC execution time of the timer to remove
        """
        ...
    
    async def clear_scheduled(self) -> None:
        """
        Unschedule ALL timers for the current key.
        """
        ...
    
    async def scheduled(self) -> List[datetime]:
        """
        List all scheduled execution times for timers on the current key.
        
        Returns:
            A list of scheduled execution times as UTC datetimes
        """
        ...
    
    def should_cancel(self) -> bool:
        """
        Check if cancellation has been requested.

        Cancellation includes both message-level cancellation (e.g., timeout)
        and partition shutdown.

        Returns:
            True if cancellation has been requested, False otherwise
        """
        ...

    async def on_cancel(self) -> None:
        """
        Waits for a cancellation signal.

        Cancellation includes both message-level cancellation (e.g., timeout)
        and partition shutdown.

        Returns:
            A coroutine that completes when cancellation is signaled.
        """
        ...
