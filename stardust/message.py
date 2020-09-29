from typing import Any, Optional

from stardust.actor_ref import ActorRef


class Message:
    def __init__(
            self,
            sender_ref: ActorRef,
            target_ref: ActorRef,
            message: Any,
            context: Optional[int] = None
    ):
        self.sender_ref = sender_ref
        self.target_ref = target_ref
        self.message = message
        self.context = context

    def __str__(self):
        return f'Message(from={self.sender_ref}, to={self.target_ref}, context={self.context}, data={self.message})'
