import operator
import uuid

from collections import deque
from threading import Thread, Condition
from multiprocessing import Queue as MPQueue

from stardust.actor_ref import ActorRef
from stardust.internal.messages import InternalSystemMessage, ActorKilled, ActorSpawned, SchedulerStarted
from stardust.internal.messages import Teardown
from stardust.internal.messages import SpawnRequest
from stardust.internal.messages import KillRequest
from stardust.internal.scheduler import Scheduler
from stardust.message import Message


class IncomingMessageManager(Thread):
    def __init__(self, system, in_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.system = system
        self.in_queue = in_queue

    def run(self) -> None:
        while True:
            message = self.in_queue.get()

            if isinstance(message, Message):
                if self.system._a_ref == message.sender_ref \
                  and self.system._a_context == message.context:

                    self.system._a_context = None
                    self.system._a_ref = None
                    self.system._result = message.message

                    with self.system._trigger:
                        self.system._trigger.notify_all()

            elif isinstance(message, InternalSystemMessage):
                if isinstance(message, Teardown):
                    return

                elif isinstance(message, SchedulerStarted):
                    self.system._active_schedulers += 1
                    with self.system._trigger:
                        self.system._trigger.notify_all()

                else:
                    self.system.events_manager.send_message(message)


class OutgoingMessageManager(Thread):
    def __init__(self, system, queues, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.system = system
        self.queues = queues
        self.messages = deque()
        self.trigger = Condition()

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

                if isinstance(message, Message):
                    self.queues[message.target_ref.scheduler_id].put(message)

                elif isinstance(message, tuple) and isinstance(message[1], InternalSystemMessage):
                    self.queues[message[0]].put(message[1])


class SystemEventsManager(Thread):
    def __init__(self, system, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trigger = Condition()
        self.messages = deque()
        self.system = system

        self.actors_per_scheduler = {
            s_id: 0
            for s_id in range(1, self.system.config.cores_count + 1)
        }

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
                    scheduler_id = min(
                        self.actors_per_scheduler.items(),
                        key=operator.itemgetter(1)
                    )[0]
                    self.system.outgoing_manager.send_message((scheduler_id, message))

                elif isinstance(message, KillRequest):
                    self.system.outgoing_manager.send_message((
                        message.actor_ref.scheduler_id,
                        message
                    ))

                elif isinstance(message, ActorSpawned):
                    self.actors_per_scheduler[message.actor_ref.scheduler_id] += 1

                    if message.parent_ref == ActorRef(0, 0, 0):
                        self.system._result = message.actor_ref

                        with self.system._trigger:
                            self.system._trigger.notify_all()

                    else:
                        self.system.outgoing_manager.send_message((
                            message.parent_ref.scheduler_id,
                            message
                        ))

                elif isinstance(message, ActorKilled):
                    self.actors_per_scheduler[message.killed_ref.scheduler_id] -= 1
                    self.system.outgoing_manager.send_message((
                        message.killer_ref.scheduler_id,
                        message
                    ))


class System:
    def __init__(self, config):
        self.tasks = deque()

        self.config = config

        self._queues = {
            i: MPQueue()
            for i in range(1, self.config.cores_count + 1)
        }
        self._queues[0] = MPQueue()

        self.schedulers = {
            i: Scheduler(i, self._queues)
            for i in range(1, self.config.cores_count + 1)
        }

        self.incoming_manager = IncomingMessageManager(self, self._queues[0])
        self.outgoing_manager = OutgoingMessageManager(self, self._queues)
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

        self.incoming_manager.start()
        self.outgoing_manager.start()
        self.events_manager.start()

        for scheduler in self.schedulers.values():
            scheduler.start()

        while self._active_schedulers < self.config.cores_count:
            with self._trigger:
                self._trigger.wait()

    def stop(self):
        self._is_running = False

        for queue in self._queues.values():
            queue.put(Teardown())

        with self.outgoing_manager.trigger:
            self.outgoing_manager.trigger.notify_all()

        with self.events_manager.trigger:
            self.events_manager.trigger.notify_all()

        self.incoming_manager.join()
        self.outgoing_manager.join()
        self.events_manager.join()

        for scheduler in self.schedulers.values():
            scheduler.join()

    def spawn(self, actor_class, *args, **kwargs):
        self.incoming_manager.in_queue.put(
            SpawnRequest(
                parent_ref=ActorRef(0, 0, 0),
                actor_class=actor_class,
                args=args,
                kwargs=kwargs
            )
        )

        with self._trigger:
            self._trigger.wait()

        result = self._result
        self._result = None
        self._a_ref = None
        self._a_context = None

        return result

    def send(self, target_ref, message):
        self.outgoing_manager.messages.append(
            Message(
                sender_ref=ActorRef(0, 0, 0),
                target_ref=target_ref,
                message=message
            )
        )

    def ask(self, target_ref, message, timeout=None):
        context = uuid.uuid4().int

        self._a_ref = target_ref
        self._a_context = context
        self._result = None

        self.outgoing_manager.send_message(
            Message(
                sender_ref=ActorRef(0, 0, 0),
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
