from datetime import datetime
from typing import List, overload

from prosody.message import Message
from prosody.state import (
    DequeDefinition,
    DequeState,
    MapDefinition,
    MapState,
    MessageDequeDefinition,
    MessageMapDefinition,
    MessageValueDefinition,
    ValueDefinition,
    ValueState,
)
from typing_extensions import TypeVar

T = TypeVar("T")
V = TypeVar("V")
P = TypeVar("P")


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

        Cancellation includes message-level cancellation (e.g., timeout) and
        partition shutdown. During shutdown, cancellation is delayed until near
        the end of the shutdown timeout to allow in-flight work to complete.

        Returns:
            True if cancellation has been requested, False otherwise
        """
        ...

    async def on_cancel(self) -> None:
        """
        Waits for a cancellation signal.

        Cancellation includes message-level cancellation (e.g., timeout) and
        partition shutdown. During shutdown, cancellation is delayed until near
        the end of the shutdown timeout to allow in-flight work to complete.

        Returns:
            A coroutine that completes when cancellation is signaled.
        """
        ...

    # Message-payload definitions listed first for overload resolution.
    @overload
    def state(
        self, definition: MessageValueDefinition[P]
    ) -> ValueState[Message[P]]: ...
    @overload
    def state(
        self, definition: MessageMapDefinition[P]
    ) -> MapState[Message[P]]: ...
    @overload
    def state(
        self, definition: MessageDequeDefinition[P]
    ) -> DequeState[Message[P]]: ...
    @overload
    def state(self, definition: ValueDefinition[T]) -> ValueState[T]: ...
    @overload
    def state(self, definition: MapDefinition[V]) -> MapState[V]: ...
    @overload
    def state(self, definition: DequeDefinition[T]) -> DequeState[T]: ...
    def state(self, definition: object) -> object:
        """Bind a registered collection for the current event attempt.

        Returns a typed handle over the collection: JSON definitions vend
        ``ValueState[T]`` / ``MapState[V]`` / ``DequeState[T]``; message
        definitions vend the same handles parameterized by ``Message[P]``. The
        handle — and any iterator it opens — is valid only within the handler
        invocation that created it; do not retain it past the handler.

        Binding an unregistered name, or a definition whose ``kind`` / ``payload``
        disagrees with the collection's durably-registered schema, raises
        :class:`~prosody.errors.PermanentStateError`.
        """
        ...
