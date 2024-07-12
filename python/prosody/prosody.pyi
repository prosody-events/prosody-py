from abc import abstractmethod
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


class AbstractMessageHandler:
    @abstractmethod
    def handle(self, message: 'Message') -> str: ...


class Prosody:
    def __init__(
            self,
            *,
            bootstrap_servers: Optional[Union[str, List[str]]] = None,
            mock: Optional[bool] = None,
            send_timeout: Optional[Union[float, timedelta]] = None,
            group_id: Optional[str] = None,
            subscribed_topics: Optional[Union[str, List[str]]] = None,
            max_uncommitted: Optional[int] = None,
            max_enqueued_per_key: Optional[int] = None,
            partition_shutdown_timeout: Optional[Union[float, timedelta]] = None,
            poll_interval: Optional[Union[float, timedelta]] = None,
            commit_interval: Optional[Union[float, timedelta]] = None
    ) -> None: ...

    async def send(self, topic: str, key: str, payload: Any) -> None: ...

    def __repr__(self) -> str: ...

    def __str__(self) -> str: ...


class Message:
    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...

    def key(self) -> str: ...

    def payload(self) -> JSONValue: ...

    def __str__(self) -> str: ...

    def __repr__(self) -> str: ...
