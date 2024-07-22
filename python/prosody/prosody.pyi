from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, List, Optional, Union, TypeAlias, Dict

# Define a JSONValue type that represents all possible JSON-serializable values
JSONValue: TypeAlias = Union[
    None,
    bool,
    int,
    float,
    str,
    List['JSONValue'],
    Dict[str, 'JSONValue']
]

# Define a Duration type alias for time-related parameters
Duration: TypeAlias = Union[float, timedelta]

# Define a StringOrList type alias for parameters that accept either a string or a list of strings
StringOrList: TypeAlias = Union[str, List[str]]


class Context:
    ...


class Message:
    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...

    def key(self) -> str: ...

    def payload(self) -> JSONValue: ...

    def __str__(self) -> str: ...

    def __repr__(self) -> str: ...


class AbstractMessageHandler(ABC):
    @abstractmethod
    async def handle(self, context: Context, message: Message) -> None: ...


class ProsodyClient:
    def __init__(
            self,
            *,
            bootstrap_servers: Optional[StringOrList] = None,
            mock: Optional[bool] = None,
            send_timeout: Optional[Duration] = None,
            group_id: Optional[str] = None,
            subscribed_topics: Optional[StringOrList] = None,
            max_uncommitted: Optional[int] = None,
            max_enqueued_per_key: Optional[int] = None,
            partition_shutdown_timeout: Optional[Duration] = None,
            poll_interval: Optional[Duration] = None,
            commit_interval: Optional[Duration] = None,
            mode: Optional[str] = None,
            retry_base: Optional[int] = None,
            max_retries: Optional[int] = None,
            max_retry_delay: Optional[Duration] = None,
            failure_topic: Optional[str] = None
    ) -> None: ...

    async def send(self, topic: str, key: str, payload: Any) -> None: ...

    def consumer_state(self) -> str: ...

    def subscribe(self, handler: AbstractMessageHandler) -> None: ...

    async def unsubscribe(self) -> None: ...

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...
