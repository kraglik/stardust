from stardust.actor_ref import ActorRef
from stardust.internal.messages import InternalSystemMessage, ActorKilled, ActorSpawned
from stardust.internal.messages import Teardown
from stardust.internal.messages import SpawnRequest
from stardust.internal.messages import KillRequest
from stardust.internal.scheduler import Scheduler
from stardust.message import Message


class MessageManager:
    def __init__(self, system):
        self.system = system

    def send(self, message):
        # Simple case
        if isinstance(message, Message):
            if message.target_ref.is_system_ref() and self.system._a_context == message.context:
                self.system._a_context = None
                self.system._a_ref = None
                self.system._result = message.message

                with self.system._trigger:
                    self.system._trigger.notify_all()

            else:
                cell = self.system.scheduler.cell_by_id[message.target_ref.actor_id]

                if not isinstance(message.message, InternalSystemMessage):
                    cell.mailbox.enqueue(message)
                else:
                    cell.mailbox.enqueue_system(message)

                if not cell.is_running and cell not in self.system.scheduler.ready_cells:
                    self.system.scheduler.ready_cells.append(cell)

                    with self.system.scheduler.trigger:
                        self.system.scheduler.trigger.notify_all()

        elif isinstance(message, ActorSpawned):
            if message.parent_ref.is_system_ref():
                self.system._result = message.actor_ref
                with self.system._trigger:
                    self.system._trigger.notify_all()
            else:
                self.system.scheduler.actor_spawned(message)

        elif isinstance(message, ActorKilled):
            if message.killer_ref.is_system_ref():
                with self.system._trigger:
                    self.system._trigger.notify_all()
            else:
                self.system.scheduler.actor_killed(message)

        else:
            self.system.events_manager.send_message(message)