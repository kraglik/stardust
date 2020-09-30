import operator
import uuid

from collections import deque
from threading import Thread, Condition
from multiprocessing import Queue as MPQueue

from stardust.actor_ref import ActorRef
from stardust.internal.message_manager import MessageManager
from stardust.internal.messages import InternalSystemMessage, ActorKilled, ActorSpawned
from stardust.internal.messages import Teardown
from stardust.internal.messages import SpawnRequest
from stardust.internal.messages import KillRequest
from stardust.internal.scheduler import Scheduler
from stardust.message import Message


class SystemEventsManager(Thread):
    def __init__(self, system, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trigger = Condition()
        self.messages = deque()
        self.system = system

    def send_message(self, message):
        self.messages.append(message)

        with self.trigger:
            self.trigger.notify_all()

    def run(self) -> None:
        while True:
            with self.trigger:
                self.trigger.wait()

            if not self.system.is_running:
                break

            while len(self.messages) > 0:
                message = self.messages.popleft()

                if isinstance(message, SpawnRequest):
                    self.system.scheduler.spawn_actor(message)

                elif isinstance(message, KillRequest):
                    self.system.scheduler.kill_actor(message)


class System:
    def __init__(self, config):
        self.tasks = deque()

        self.config = config

        self.scheduler = Scheduler(self)
        self._messages = deque()
        self.message_manager = MessageManager(self)
        self.events_manager = SystemEventsManager(self)
        self._a_context = None
        self._a_ref = None
        self._result = None
        self._trigger = Condition()

        self._is_running = False
        self._active_schedulers = 0

    @property
    def is_running(self):
        return self._is_running

    def start(self):
        self._is_running = True

        self.events_manager.start()
        self.scheduler.start()

        with self._trigger:
            self._trigger.wait()

    def stop(self):
        self._is_running = False

        with self.events_manager.trigger:
            self.events_manager.trigger.notify_all()

        self.events_manager.join()

        with self.scheduler.trigger:
            self.scheduler.trigger.notify_all()

        self.scheduler.join()

    def spawn(self, actor_class, *args, **kwargs):
        return self.scheduler.spawn_actor(
            SpawnRequest(
                parent_ref=ActorRef(0, 0),
                actor_class=actor_class,
                args=args,
                kwargs=kwargs
            )
        )

    def send(self, target_ref, message):
        self.message_manager.send(
            Message(
                sender_ref=ActorRef(0, 0),
                target_ref=target_ref,
                message=message
            )
        )

    def ask(self, target_ref, message, timeout=None):
        context = uuid.uuid4().int

        self._a_ref = target_ref
        self._a_context = context
        self._result = None

        self.message_manager.send(
            Message(
                sender_ref=ActorRef(0, 0),
                target_ref=target_ref,
                message=message,
                context=context
            )
        )
        with self._trigger:
            self._trigger.wait(timeout)

        result = self._result
        self._result = None

        return result
