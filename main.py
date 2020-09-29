import time
import asyncio

from stardust import Actor, System, Config
from stardust.messages import WakeUp


class Ping(Actor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.count = 0

    async def receive(self, message, sender):
        await asyncio.sleep(0.1)

        self.count += 1

        if message == 'count':
            await self.reply(self.count)

        if message == 'ping':
            await self.send(self.ref(), 'pong')

        elif message == 'pong':
            await self.send(self.ref(), 'ping')

        elif isinstance(message, WakeUp):
            await self.send(self.ref(), 'ping')


def main():
    system = System(
        config=Config()
    )

    system.start()
    ping = system.spawn(Ping)
    print('ping address is', ping)

    time.sleep(1)

    print('count:', system.ask(ping, 'count'))

    system.stop()


if __name__ == '__main__':
    main()
