from abc import ABC, abstractmethod


class AbstractMessageHandler(ABC):
    @abstractmethod
    async def handle(self, context, message):
        pass
