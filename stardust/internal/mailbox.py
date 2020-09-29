from collections import deque


class Mailbox:
    def __init__(self):
        self._messages = deque()
        self._system_messages = deque()
        self._preserved_messages = deque()
        self._preserved_system_messages = deque()

    def enqueue(self, message):
        self._messages.append(message)

    def enqueue_system(self, message):
        self._system_messages.append(message)

    def flush(self):
        self._messages.extendleft(self._preserved_messages)
        self._system_messages.extendleft(self._preserved_system_messages)

        self._preserved_messages.clear()
        self._preserved_system_messages.clear()

    def __len__(self):
        return len(self._messages) + len(self._system_messages)

    def pop(self):
        if len(self._system_messages) > 0:
            return self._system_messages.popleft()

        elif len(self._messages) > 0:
            return self._messages.popleft()

        else:
            raise RuntimeError('Mailbox is empty!')

