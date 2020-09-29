import uuid
from collections import deque

from stardust.actor_ref import ActorRef
from stardust.internal.mailbox import Mailbox


class Cell:
    def __init__(
            self,
            actor_class: type,
            actor_args: list,
            actor_kwargs: dict,
            actor_ref: ActorRef
    ):
        self.actor = actor_class(ref=actor_ref, *actor_args, **actor_kwargs)
        self.ref = actor_ref
        self.mailbox = Mailbox()
        self.future = None
        self._coroutine = None
        self._behaviour = self.actor.receive
        self.a_context = None
        self.a_ref = None
        self.sender = None
        self.context = None
        self.current_message = None
        self.is_running = False

    def receive(self, message):
        self.mailbox.enqueue(message)

    def receive_system(self, message):
        self.mailbox.enqueue_system(message)

    async def run(self):
        self.is_running = True

        if len(self.mailbox):
            message = self.mailbox.pop()

            self.sender = message.sender_ref
            self.context = message.context
            self.current_message = message

            try:
                await self._behaviour(message.message, message.sender_ref)
            except Exception:
                pass

            self.sender = None
            self.context = None
            self.current_message = None

        self.is_running = False
