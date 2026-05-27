from abc import ABC, abstractmethod
import json
from time import time
from typing import Protocol, TypedDict, cast

from reification import Reified

from bourgade.utils.dicts import optional_entry

type JsonDict = dict[str, object]

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

    async def trigger(self, event_bus: "EventBus[EventBusSetupOptions]", message: bytes) -> None:
        """
        Builds, and hydrates a new event object from RabbitMQ deliver,
        and triggers `handle` within this handler.

        :param EventBus event_bus: The event bus containing this handler
        :param Basic.Deliver deliver: The RabbitMQ deliver object considered to be an event for the handler
        :param bytes message: The RabbitMQ message bytes for event hydration
        """
        event: E = cast(E, event_bus.deserialize_event(message))
        await self.handle(event=event)

    @abstractmethod
    async def handle(self, event: E) -> None:
        """
        Handles the event when triggered.
        Override this method to define handling logic.

        :param E event: The event to handle
        """
        ...


class EventBusSetupOptions(TypedDict):
    pass


class EventBus[TSetupOptions: EventBusSetupOptions](ABC):
    """
    Connects to RabbitMQ, declares its abstractions,
    and manages RabbitMQ messages, passing them to handlers.
    """

    all_catch_event_handler: AllCatchEventHandler | None
    event_handlers: dict[str, EventHandler["Event"]]

    def __init__(self) -> None:
        self.all_catch_event_handler = None
        self.event_handlers = {}

    def deserialize_event(self, message_bytes: bytes) -> Event:
        event_name = Event.get_event_name_from_bytes(message_bytes)
        ThisEvent: type[Event] = self.event_handlers[event_name].get_event_type()
        happened_at: int = int(time() * 1000)
        event = ThisEvent(event_bus=cast(EventBus[EventBusSetupOptions], self), happened_at=happened_at)
        event.hydrate(message=message_bytes)

        return event

    async def trigger(self, message_bytes: bytes) -> None:
        event_name: str = Event.get_event_name_from_bytes(message_bytes)

        if event_name in self.event_handlers:
            event_handler: EventHandler[Event] = self.event_handlers[event_name]
            await event_handler.trigger(event_bus=cast(EventBus[EventBusSetupOptions], self), message=message_bytes)
        elif self.all_catch_event_handler is not None:
            self.all_catch_event_handler(event_name, message_bytes)
        else:
            raise ValueError(f"There is no event handler for '{event_name}'.")

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
    async def setup(self, options: TSetupOptions) -> None:
        """
        Used for setting up the bus. It is called automatically
        when creating the bus via Bourgade 'bus' helper.
        Here, the bus can create message exchange provider-specific clients,
        establish non-blocking connections, and etc.

        Notice that the listening/subscriber loop should not be here.
        Instead, implement it in `listen()` abstract method.
        """

    @abstractmethod
    async def listen(self) -> None:
        """
        Starts listening for messages.
        This method is blocking.
        """

    @abstractmethod
    async def dispatch(self, event: "Event") -> None:
        """
        Dispatches an event to exchange.

        :param Event event: The event to dispatch
        """


class Event(ABC):
    """
    Events in Bourgade are Json-based.
    So to use Bourgade events, you must implement get/set contents of the object.
    """

    event_bus: EventBus[EventBusSetupOptions]
    happened_at: int
    sid: str | None

    def __init__(self, event_bus: EventBus[EventBusSetupOptions], happened_at: int) -> None:
        self.event_bus = event_bus
        self.happened_at = happened_at
        self.sid = None

    @staticmethod
    def get_event_name_from_bytes(message_bytes: bytes) -> str:
        message: str = message_bytes.decode()
        payload: JsonDict = cast(JsonDict, json.loads(message))
        header: JsonDict = cast(JsonDict, payload["header"])
        return cast(str, header.get("event"))

    @abstractmethod
    def get_content_as_dict(self) -> JsonDict: ...

    @abstractmethod
    def set_content_from_dict(self, content: JsonDict) -> None: ...

    def hydrate(self, message: bytes) -> None:
        payload: JsonDict = cast(JsonDict, json.loads(message))
        header: JsonDict = cast(JsonDict, payload["header"])
        content: JsonDict = cast(JsonDict, payload["content"])
        self.sid = cast(str, header.get("sid"))
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
    def create(cls, event_bus: EventBus[EventBusSetupOptions], content: JsonDict) -> "Event":
        happened_at: int = int(time() * 1000)
        event = cls(event_bus=event_bus, happened_at=happened_at)
        event.set_content_from_dict(content=content)
        return event
