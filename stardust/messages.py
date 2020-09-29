class SystemMessage:
    pass


class Kill(SystemMessage):
    def __str__(self):
        return 'Kill'

    def __repr__(self):
        return str(self)


class WakeUp(SystemMessage):
    def __str__(self):
        return 'WakeUp'

    def __repr__(self):
        return str(self)

