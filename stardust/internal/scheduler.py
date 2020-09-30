import asyncio
import functools
import time

from collections import deque
from threading import Thread, Condition
from typing import Dict

from stardust.actor_ref import ActorRef
from stardust.internal.cell import Cell
from stardust.internal.messages import Teardown
from stardust.internal.messages import InternalSystemMessage
from stardust.internal.messages import SpawnRequest
from stardust.internal.messages import KillRequest
from stardust.internal.messages import ActorSpawned
from stardust.internal.messages import ActorKilled
from stardust.message import Message
from stardust.messages import SystemMessage, WakeUp

_SCHEDULER = None


class AIOThread(Thread):
    def __init__(self, scheduler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scheduler = scheduler
        self.loop = None

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

            time.sleep(0.01)

            with self.scheduler.trigger:
                self.scheduler.trigger.notify_all()

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


class Scheduler(Thread):
    def __init__(self, system, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.system = system

        self.actor_counter = 1

        self.cell_by_id = dict()
        self.ready_cells = deque()

        self.trigger = Condition()
        self.aio_thread = AIOThread(self)

    def run(self) -> None:
        self.setup()

        while self.system.is_running:
            with self.trigger:
                self.trigger.wait()

            if not self.system.is_running:
                break

            self.handle_cells()

        self.teardown()

    def setup(self):
        global _SCHEDULER
        _SCHEDULER = self

        self.aio_thread.start()

        with self.trigger:
            self.trigger.wait()

        with self.system._trigger:
            self.system._trigger.notify_all()

    def teardown(self):
        self.aio_thread.stop()
        self.aio_thread.join()

    def handle_cells(self):
        while len(self.ready_cells) > 0:

            cell = self.ready_cells.popleft()

            if cell.is_running:
                continue

            if len(cell.mailbox) > 0:
                self.aio_thread.add_task(cell.run(), self.task_done_callback(cell))

    def task_done_callback(self, cell):
        def _callback(_):
            if cell.ref.actor_id in self.cell_by_id:
                if not cell.is_running and cell not in self.ready_cells:
                    self.ready_cells.append(cell)

                    with self.trigger:
                        self.trigger.notify_all()

        return _callback

    def spawn_actor(self, request: SpawnRequest):
        ref = ActorRef(0, self.actor_counter)
        cell = Cell(request.actor_class, request.args, request.kwargs, ref)

        self.cell_by_id[self.actor_counter] = cell
        self.ready_cells.append(cell)

        with self.trigger:
            self.trigger.notify_all()

        self.actor_counter += 1

        self.system.message_manager.send(
            Message(
                sender_ref=ActorRef(0, 0),
                target_ref=ref,
                message=WakeUp()
            )
        )
        self.system.message_manager.send(
            ActorSpawned(
                request.parent_ref,
                ref
            )
        )

        return ref

    def kill_actor(self, request: KillRequest):
        cell = self.cell_by_id[request.actor_ref.actor_id]
        del self.cell_by_id[request.actor_ref.actor_id]

        if cell in self.ready_cells:
            self.ready_cells.remove(cell)

        if request.actor_ref == request.killer_ref:
            self.aio_thread.loop.call_soon_threadsafe(
                lambda: cell.future.set_result(None)
            )
        else:
            self.system.message_manager.send(
                ActorKilled(
                    killer_ref=request.killer_ref,
                    killed_ref=request.actor_ref
                )
            )

    def actor_spawned(self, message: ActorSpawned):
        cell = self.cell_by_id[message.parent_ref.actor_id]

        self.aio_thread.loop.call_soon_threadsafe(
            lambda: cell.future.set_result(message.actor_ref)
        )

    def actor_killed(self, message):
        cell = self.cell_by_id[message.killer_ref.actor_id]

        self.aio_thread.loop.call_soon_threadsafe(
            lambda: cell.future.set_result(None)
        )


def get_scheduler():
    return _SCHEDULER
