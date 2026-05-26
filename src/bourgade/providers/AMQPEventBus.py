import asyncio
import logging
import sys
from typing import cast, override

from aio_pika import Message, connect_robust
from aio_pika.abc import (
        AbstractChannel, 
        AbstractConnection, 
        AbstractExchange, 
        AbstractIncomingMessage, 
        AbstractMessage, 
        AbstractQueue, 
        ExchangeType,
)
from bourgade import Event, EventBus, EventBusSetupOptions, EventHandler

logger = logging.getLogger("Bourgade over RabbitMQ")


class RabbitMQEventBusSetupOptions(EventBusSetupOptions):
    host: str
    username: str
    password: str
    exchange_name: str
    queue_name: str
    connection_delay: int
    connection_retries: int
    connection_retry_interval: int


class RabbitMQEventBus(EventBus[RabbitMQEventBusSetupOptions]):
    connection: AbstractConnection
    channel: AbstractChannel
    exchange: AbstractExchange
    queue: AbstractQueue

    @override
    async def setup(self, options: RabbitMQEventBusSetupOptions) -> None:
        await asyncio.sleep(options['connection_delay'])
        connection_retries_left: int = options['connection_retries']
        while connection_retries_left > 0:
            try:
                self.connection = await connect_robust(
                    host=options['host'],
                    login=options['username'],
                    password=options['password'],
                )
                self.channel = await self.connection.channel()
                _ = await self.channel.set_qos(prefetch_count=1)
                self.exchange = await self.channel.declare_exchange(
                    name=options['exchange_name'],
                    type=ExchangeType.TOPIC,
                    passive=False,
                    durable=True,
                    auto_delete=False,
                )

                self.queue = await self.channel.declare_queue(
                    name=options['queue_name'],
                    auto_delete=True
                )

            except Exception:
                await asyncio.sleep(options['connection_retry_interval'])
                connection_retries_left -= 1

        raise ValueError(
            "Bourgade connection to RMQ failed after several retries."
        ) from sys.last_exc

    @override
    async def listen(self) -> None:
        if self.all_catch_event_handler is None:
            for event_handler in self.event_handlers.values():
                event_name: str = event_handler.get_event_type().get_event_name()
                _ = await self.queue.bind(
                    exchange=self.exchange, routing_key=event_name
                )
        else:
            _ = await self.queue.bind(exchange=self.exchange, routing_key="#")
        async with self.queue.iterator() as queue_iterator:
            async for amqp_message in queue_iterator:
                async with amqp_message.process():
                    await self.__consume(amqp_message=amqp_message)

    @override
    async def dispatch(self, event: Event) -> None:
        await self.dispatch_raw(
            tag=event.get_event_name(), message_bytes=event.serialize()
        )

    async def dispatch_raw(
        self,
        tag: str,
        message_bytes: bytes,
        content_type: str = "application/json",
        content_encoding: str = "utf-8",
    ) -> None:
        """
        Dispatches a message with a tag, and bytes.
        Use it to avoid using event abstractions for more complex logic.

        :param str tag: The tag string for the message
        :param bytes message_bytes: The message content
        """

        amqp_message: AbstractMessage = Message(
            body=message_bytes,
            content_type=content_type,
            content_encoding=content_encoding,
        )
        _ = await self.exchange.publish(
            message=amqp_message,
            routing_key=tag,
            mandatory=False,
        )

    async def __consume(
        self,
        amqp_message: AbstractIncomingMessage,
    ) -> None:
        """
        Consumes a RabbitMQ message,
        finds a handler for an event this message represends,
        and triggers the handler to handle the event.
        Used in RabbitMQ `basic_consume` method, and never outside.

        :param EventBus event_bus: The event bus of the handler
        :param dict[str, EventHandler["Event"]] event_handlers: The dictionary of handlers (`routing_key`: `handler`)
        :param BlockingChannel channel: The RabbitMQ channel
        :param Basic.Deliver deliver: The RabbitMQ deliver
        :param BasicProperties _: The RabbitMQ properties, unused here
        :param bytes message: The message body
        """
        routing_key: str | None = amqp_message.routing_key
        message: bytes = amqp_message.body

        if routing_key is None:
            raise ValueError("Consumed message does not have a routing key.")

        logger.info("[RECV] %s", routing_key)

        try:
            if routing_key in self.event_handlers:
                event_handler: EventHandler[Event] = self.event_handlers[routing_key]
                await event_handler.trigger(event_bus=cast(EventBus[EventBusSetupOptions], self), message=message)
            elif self.all_catch_event_handler is not None:
                self.all_catch_event_handler(
                    event_name=routing_key, message_bytes=message
                )
            else:
                raise ValueError(f"There is no event handler for '{routing_key}'.")
        except Exception as exception:
            logger.error(
                msg="Event is NACK because of an exception.", exc_info=exception
            )



