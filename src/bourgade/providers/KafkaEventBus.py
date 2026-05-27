import logging
from typing import cast, override
from bourgade import Event, EventBus, EventBusSetupOptions
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = logging.getLogger("Bourgade over Kafka")


class KafkaEventBusSetupOptions(EventBusSetupOptions):
    bootstrap_servers: str
    topic: str
    group_id: str | None


class KafkaEventBus(EventBus[KafkaEventBusSetupOptions]):
    producer: AIOKafkaProducer
    consumer: AIOKafkaConsumer
    topic: str

    @override
    async def setup(self, options: KafkaEventBusSetupOptions) -> None:
        bootstrap_servers = options['bootstrap_servers']
        group_id = options['group_id']
        self.topic = options['topic']

        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=str.encode,
            value_serializer=self.serialize_value
        )

        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            key_deserializer=bytes.decode,
        )

        await self.producer.start()
        await self.consumer.start()

    @override
    async def listen(self) -> None:
        async for message in self.consumer:
            await self.trigger(cast(bytes, message.value))

    @override
    async def dispatch(self, event: Event) -> None:
        await self.producer.send(
            topic=self.topic,
            key=event.get_event_name(),
            value=event
        )
        await self.producer.flush()

    def serialize_value(self, value: object) -> bytes:
        if not isinstance(value, Event):
            raise ValueError(f'Only event objects can be serialized, but {type(value)} passed.')

        return value.serialize()



