class ActorRef:
    def __init__(self, system_id, scheduler_id, actor_id):
        self.system_id = system_id
        self.scheduler_id = scheduler_id
        self.actor_id = actor_id

    def __str__(self):
        return f'<{self.system_id}.{self.scheduler_id}.{self.actor_id}>'

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return (
            type(other) == ActorRef and
            self.system_id == other.system_id and
            self.scheduler_id == other.scheduler_id and
            self.actor_id == other.actor_id
        )

    def is_system_ref(self):
        return self.scheduler_id == 0 and self.actor_id == 0
