import asyncio
import functools

from collections import deque

from multiprocessing import Process, Queue as MPQueue
from threading import Thread, Condition
from typing import Dict

from stardust.actor_ref import ActorRef
from stardust.internal.cell import Cell
from stardust.internal.messages import Teardown, InternalSystemMessage, SpawnRequest, KillRequest, ActorSpawned, \
    ActorKilled, SchedulerStarted
from stardust.message import Message
from stardust.messages import SystemMessage, WakeUp

_SCHEDULER = None


class IncomingEventManager(Thread):
    def __init__(
            self,
            scheduler: 'Scheduler',
            in_queue: MPQueue,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.scheduler = scheduler
        self.in_queue: MPQueue = in_queue

    def run(self) -> None:
        while True:
            # Wait for message
            message = self.in_queue.get(block=True)

            # Stop if system termination is requested
            if isinstance(message, Teardown):
                break

            # Handle system requests
            elif isinstance(message, InternalSystemMessage):
                self.scheduler.system_tasks.append(message)

                with self.scheduler.loop_trigger:
                    self.scheduler.loop_trigger.notify_all()

            # We've got a simple message here
            elif isinstance(message, Message):
                self.scheduler.send_message(message)

        self.scheduler.running = False

        with self.scheduler.loop_trigger:
            self.scheduler.loop_trigger.notify_all()


class OutgoingEventManager(Thread):
    def __init__(
            self,
            scheduler: 'Scheduler',
            queues: Dict[int, MPQueue],
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.scheduler = scheduler
        self.queues = queues

        self.trigger = Condition()
        self.messages = deque()

    def send_message(self, message: Message) -> None:
        self.messages.append(message)

        with self.trigger:
            self.trigger.notify_all()

    def run(self) -> None:

        # Run while scheduler is alive
        while True:
            # Got new message to check or scheduler has stopped
            with self.trigger:
                self.trigger.wait()

            if not self.scheduler.running:
                # We have not to much work to do, this manager is only responsible for outgoing messages
                break

            # There may be more than one message since last notification or it might be a false alarm
            while len(self.messages) > 0:
                message = self.messages.popleft()

                # Normal messages are directly sent to the actors
                if isinstance(message, Message):
                    self.queues[message.target_ref.scheduler_id].put(message)

                # System messages must be handled by system ('Kill' messages, for example)
                elif isinstance(message, InternalSystemMessage):
                    self.queues[0].put(message)


class AIOThread(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = None

    # def add_task(self, coro):
    #     f = functools.partial(self.loop.create_task, coro)
    #     return self.loop.call_soon_threadsafe(f)

    def add_task(self, coro, callback=None):
        def _callback():
            task = self.loop.create_task(coro)
            if callback is not None:
                task.add_done_callback(callback)

        return self.loop.call_soon_threadsafe(_callback)

    def run(self) -> None:
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_forever()
            pending = asyncio.Task.all_tasks(self.loop)
            self.loop.run_until_complete(asyncio.gather(*pending))
            self.loop.close()
        except Exception:
            pass

    def stop(self):
        while True:
            try:
                self.loop.call_soon_threadsafe(self.loop.stop)
                break
            except AttributeError:
                continue


class Scheduler(Process):
    def __init__(
            self,
            scheduler_id: int,
            queues: Dict[int, MPQueue],
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.id: int = scheduler_id
        self.queues: Dict[int, MPQueue] = queues
        self.ready_cells = deque()
        self.cell_by_id: Dict[int, Cell] = dict()
        self.actor_counter = 1
        self.system_tasks = deque()

        self.loop_trigger = None
        self.running = False
        self.aio_thread = None

        self.incoming_manager = None
        self.outgoing_manager = None

    def run(self) -> None:
        self.setup()

        self.outgoing_manager.send_message(SchedulerStarted())

        while True:
            with self.loop_trigger:
                while self.running and len(self.system_tasks) == 0 and len(self.ready_cells) == 0:
                    self.loop_trigger.wait()

            if not self.running:
                break

            # Until all system tasks are done
            if len(self.system_tasks) > 0:
                self.handle_system_tasks()

            # Handle all of the ready cells
            if len(self.ready_cells) > 0:
                self.handle_cells()

        self.teardown()

    def task_done_callback(self, cell):
        def _callback(_):
            if cell.ref.actor_id in self.cell_by_id:
                if not cell.is_running and cell not in self.ready_cells:
                    self.ready_cells.append(cell)

                    with self.loop_trigger:
                        self.loop_trigger.notify_all()

        return _callback

    def handle_cells(self):
        # for every actor that waits for execution
        while len(self.ready_cells):
            # We need to remove such an actor from the set of ready cells
            cell = self.ready_cells.popleft()

            if cell.is_running:
                continue

            # And then we need to process it and return it back into the set of ready cells
            if len(cell.mailbox) > 0:
                self.aio_thread.add_task(cell.run(), self.task_done_callback(cell))

                with self.loop_trigger:
                    self.loop_trigger.notify_all()

    def setup(self):
        global _SCHEDULER

        _SCHEDULER = self

        self.loop_trigger = Condition()
        self.running = True

        self.incoming_manager = IncomingEventManager(self, self.queues[self.id])
        self.outgoing_manager = OutgoingEventManager(self, self.queues)
        self.aio_thread = AIOThread()

        self.incoming_manager.start()
        self.outgoing_manager.start()
        self.aio_thread.start()

    def teardown(self):
        self.incoming_manager.join()

        with self.outgoing_manager.trigger:
            self.outgoing_manager.trigger.notify_all()

        self.outgoing_manager.join()

        self.aio_thread.stop()
        self.aio_thread.join()

    def handle_system_tasks(self):
        while len(self.system_tasks):
            message = self.system_tasks.popleft()

            if isinstance(message, SpawnRequest):
                self.spawn_actor(message)

            elif isinstance(message, KillRequest):
                self.kill_actor(message)

            elif isinstance(message, ActorSpawned):
                cell = self.cell_by_id[message.parent_ref.actor_id]

                cell.future.set_result(message.actor_ref)

            elif isinstance(message, ActorKilled):
                cell = self.cell_by_id[message.killer_ref.actor_id]

                cell.future.set_result(None)

    def send_message(self, message):
        actor_id = message.target_ref.actor_id
        cell = self.cell_by_id[actor_id]

        if isinstance(message.message, SystemMessage):
            cell.receive_system(message)
        else:
            cell.receive(message)

        if not cell.is_running and cell not in self.ready_cells:
            self.ready_cells.append(self.cell_by_id[actor_id])

        # This method might be called from another thread, we need to notify scheduler
        with self.loop_trigger:
            self.loop_trigger.notify_all()

    def spawn_actor(self, request: SpawnRequest):
        ref = ActorRef(0, self.id, self.actor_counter)
        cell = Cell(request.actor_class, request.args, request.kwargs, ref)

        self.cell_by_id[self.actor_counter] = cell
        self.ready_cells.append(cell)

        self.actor_counter += 1

        self.send_message(
            Message(
                sender_ref=ActorRef(0, 0, 0),
                target_ref=ref,
                message=WakeUp()
            )
        )
        self.outgoing_manager.send_message(
            ActorSpawned(
                request.parent_ref,
                ref
            )
        )

    def kill_actor(self, request: KillRequest):
        cell = self.cell_by_id[request.actor_ref.actor_id]
        del self.cell_by_id[request.actor_ref.actor_id]
        self.ready_cells.remove(cell)


def get_scheduler():
    return _SCHEDULER
