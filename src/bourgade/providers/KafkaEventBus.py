from typing import override
from bourgade import Event, EventBus


class KafkaEventBus(EventBus):
    @override
    @classmethod
    async def create(
        cls, 
        host: str, 
        username: str, 
        password: str, 
        exchange_name: str, 
        queue_name: str, 
        *, 
        connection_delay: int = 0, 
        connection_retries: int = 10, 
        connection_retry_interval: int = 3
    ) -> EventBus:
        pass

    @override
    async def start_listening(self) -> None:
        pass

    @override
    async def dispatch(self, event: Event) -> None:
        pass
