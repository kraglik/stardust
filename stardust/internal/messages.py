class InternalSystemMessage:
    pass


class Teardown(InternalSystemMessage):
    pass


class SchedulerStarted(InternalSystemMessage):
    pass


class SpawnRequest(InternalSystemMessage):
    def __init__(self, parent_ref, actor_class, args, kwargs):
        self.parent_ref = parent_ref
        self.actor_class = actor_class
        self.args = args
        self.kwargs = kwargs


class KillRequest(InternalSystemMessage):
    def __init__(self, killer_ref, actor_ref):
        self.killer_ref = killer_ref
        self.actor_ref = actor_ref


class ActorKilled(InternalSystemMessage):
    def __init__(self, killer_ref, killed_ref):
        self.killer_ref = killer_ref
        self.killed_ref = killed_ref


class ActorSpawned(InternalSystemMessage):
    def __init__(self, parent_ref, actor_ref):
        self.parent_ref = parent_ref
        self.actor_ref = actor_ref
