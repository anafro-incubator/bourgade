from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
from time import time
from typing import Any, Protocol, cast

from aio_pika.abc import (
    AbstractIncomingMessage,
)
from reification import Reified

from bourgade.utils.dicts import optional_entry


class AllCatchEventHandler(Protocol):
    def __call__(self, event_name: str, message_bytes: bytes) -> None: ...


class EventHandler[E: Event = Event](ABC, Reified):
    """
    A base of all classes handling events.
    Specify event type in generic type (see Event class docs).
    Write what your app should do on event in overriden `handle` method.
    After creating a handler, add it to the event bus.
    """

    @classmethod
    def get_event_type(cls) -> type[E]:
        """
        Returns a type of the event this handler handles.
        Used for automagical event creation keeping event hydration
        outside Event class.

        :returns: The exact event type.
        """
        return cast(type[E], cls.targ)

    async def trigger(self, event_bus: "EventBus", message: bytes) -> None:
        """
        Builds, and hydrates a new event object from RabbitMQ deliver,
        and triggers `handle` within this handler.

        :param EventBus event_bus: The event bus containing this handler
        :param Basic.Deliver deliver: The RabbitMQ deliver object considered to be an event for the handler
        :param bytes message: The RabbitMQ message bytes for event hydration
        """
        ThisEvent: type[E] = self.get_event_type()
        happened_at: int = int(time() * 1000)
        event = ThisEvent(event_bus=event_bus, happened_at=happened_at)
        event.hydrate(message=message)
        await self.handle(event=event)

    @abstractmethod
    async def handle(self, event: E) -> None:
        """
        Handles the event when triggered.
        Override this method to define handling logic.

        :param E event: The event to handle
        """
        ...


@dataclass
class EventBus(ABC):
    """
    Connects to RabbitMQ, declares its abstractions,
    and manages RabbitMQ messages, passing them to handlers.
    """

    all_catch_event_handler: AllCatchEventHandler | None
    event_handlers: dict[str, EventHandler["Event"]]

    @abstractmethod
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
        connection_retry_interval: int = 3,
    ) -> "EventBus":
        """
        Creates a new event bus.

        :param str host: The RabbitMQ host
        :param str username: The RabbitMQ username
        :param str password: The RabbitMQ password
        :param str exchange_name: The RabbitMQ exchange name containing events across the infrastructure
        :param str host: The RabbitMQ queue name consuming events within the app
        """

    def register_handler[E: Event](self, event_handler: EventHandler[E]) -> None:
        """
        Registers a new handler, so when bus starts listening to messages,
        it could be recognized by the bus, and got triggered by events.

        :param EventHandler[E] event_handler: The event handler to register
        """
        event_type: type[Event] = cast(type[Event], event_handler.targ)

        self.event_handlers[event_type.get_event_name()] = cast(
            EventHandler[Event], event_handler
        )

    def set_all_catch_handler(
        self, all_catch_event_handler: AllCatchEventHandler
    ) -> None:
        self.all_catch_event_handler = all_catch_event_handler

    @abstractmethod
    async def start_listening(self) -> None:
        """
        Starts listening for RabbitMQ messages.
        This method is blocking.
        """

    @abstractmethod
    async def dispatch(self, event: "Event") -> None:
        """
        Dispatches an event to RabbitMQ exchange.

        :param Event event: The event to dispatch
        """



class Event(ABC):
    """
    Events in Bourgade are Json-based.
    So to use Bourgade events, you must implement get/set contents of the object.
    """

    event_bus: EventBus
    happened_at: int
    sid: str | None

    def __init__(self, event_bus: EventBus, happened_at: int) -> None:
        self.event_bus = event_bus
        self.happened_at = happened_at
        self.sid = None

    @abstractmethod
    def get_content_as_dict(self) -> dict[str, Any]: ...

    @abstractmethod
    def set_content_from_dict(self, content: dict[str, Any]) -> None: ...

    def hydrate(self, message: bytes) -> None:
        payload: dict[str, Any] = json.loads(message)
        header: dict[str, Any] = payload["header"]
        content: dict[str, Any] = payload["content"]
        self.sid = header.get("sid")
        self.set_content_from_dict(content=content)

    def serialize(self) -> bytes:
        return json.dumps(
            {
                "header": {
                    "happenedAt": self.happened_at,
                    **optional_entry("sid", self.sid),
                },
                "content": self.get_content_as_dict(),
            }
        ).encode("utf-8")

    @staticmethod
    @abstractmethod
    def get_event_name() -> str:
        """
        Returns an event name.
        The event name is used as routing keys in RabbitMQ.
        If your class is named `UserCreatedEvent`, return "user.created" from here.

        :returns str: The event name
        """
        ...

    @classmethod
    def create(cls, event_bus: EventBus, content: dict[str, Any]) -> "Event":
        happened_at: int = int(time() * 1000)
        event = cls(event_bus=event_bus, happened_at=happened_at)
        event.set_content_from_dict(content=content)
        return event
