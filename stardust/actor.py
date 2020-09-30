import asyncio
import uuid

from stardust.actor_ref import ActorRef
from stardust.internal.scheduler import get_scheduler, Scheduler
from stardust.message import Message
from stardust.internal.messages import KillRequest, SpawnRequest, InternalSystemMessage

__all__ = ['Actor']


class Actor:

    def __init__(self, ref=None):
        self._ref = ref

    def ref(self) -> ActorRef:
        return self._ref

    def get_context(self):
        pass

    def _cell(self):
        return get_scheduler().cell_by_id[self._ref.actor_id]

    async def receive(self, message, sender):
        raise NotImplementedError

    async def send(self, actor_ref, message):
        msg = Message(
            self._ref,
            actor_ref,
            message,
            None
        )

        get_scheduler().system.message_manager.send(msg)

    async def reply(self, message):
        cell = self._cell()

        msg = Message(
            sender_ref=self._ref,
            target_ref=cell.sender,
            message=message,
            context=cell.context
        )

        get_scheduler().system.message_manager.send(msg)

    async def spawn(self, actor_class, *args, **kwargs):
        msg = SpawnRequest(
            self._ref,
            actor_class,
            args,
            kwargs
        )

        future = asyncio.Future()

        cell = self._cell()

        def _callback(_):
            cell.a_context = None
            cell.a_ref = None
            cell.future = None

        cell.future = future
        get_scheduler().system.message_manager.send(msg)

        future.add_done_callback(_callback)

        return await future

    async def ask(self, actor_ref, message, timeout):
        context = uuid.uuid4().int

        msg = Message(
            self._ref,
            actor_ref,
            message,
            context=context
        )

        cell = self._cell()
        cell.a_context = context
        cell.a_ref = actor_ref

        future = asyncio.Future()

        def _callback(_):
            cell.a_context = None
            cell.a_ref = None
            cell.future = None

        future.add_done_callback(_callback)

        cell.future = future
        get_scheduler().system.message_manager.send(msg)

        return await asyncio.wait_for(future, timeout)

    async def kill(self, actor_ref):
        msg = KillRequest(
            self._ref,
            actor_ref
        )

        future = asyncio.Future()

        cell = self._cell()

        cell.future = future
        get_scheduler().system.message_manager.send(msg)

        def _callback(_):
            cell.a_context = None
            cell.a_ref = None
            cell.future = None

        future.add_done_callback(_callback)

        return await future

    async def become(self, new_behaviour):
        pass

    async def unbecome(self):
        pass

    async def stash(self):
        cell = self._cell()

        if isinstance(cell.current_message.message, InternalSystemMessage):
            cell.mailbox._preserved_system_messages.appendleft(cell.current_message)
        else:
            cell.mailbox._preserved_messages.appendleft(cell.current_message)

    async def unstash(self):
        cell = self._cell()
        scheduler = get_scheduler()

        cell.mailbox.flush()

        if cell not in scheduler.ready_cells:
            scheduler.ready_cells.append(self._cell())

        with scheduler.loop_trigger:
            scheduler.loop_trigger.notify()

    async def nice(self):
        await asyncio.sleep(0)
