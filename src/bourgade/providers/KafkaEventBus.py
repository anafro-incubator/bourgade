import logging
from typing import override
from bourgade import Event, EventBus, EventBusSetupOptions

logger = logging.getLogger("Bourgade over Kafka")


class KafkaEventBusSetupOptions(EventBusSetupOptions):
    pass

class KafkaEventBus(EventBus[KafkaEventBusSetupOptions]):
    @override
    async def setup(self, options: KafkaEventBusSetupOptions) -> None:
        pass

    @override
    async def listen(self) -> None:
        pass

    @override
    async def dispatch(self, event: Event) -> None:
        pass
