from abc import ABC, abstractmethod


class AbstractMessageHandler(ABC):
    @abstractmethod
    async def handle(self, message):
        pass
