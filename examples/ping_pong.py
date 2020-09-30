import time
import asyncio

from stardust import Actor, System, Config
from stardust.messages import WakeUp


class Pong(Actor):
    async def receive(self, message, sender):
        if message == 'ping':
            print('ping')
            await self.send(sender, 'pong')


class Ping(Actor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.count = 0
        self.pong = None

    async def receive(self, message, sender):
        await asyncio.sleep(0.1)

        if message == 'count':
            await self.reply(self.count)

        if message == 'pong' and self.count < 10:
            print('pong')
            self.count += 1
            await self.send(self.pong, 'ping')

        elif isinstance(message, WakeUp):
            self.pong = await self.spawn(Pong)
            print('pong address is', self.pong)
            await self.send(self.pong, 'ping')

        if self.count == 10 and self.pong is not None:
            await self.kill(self.pong)
            self.pong = None


def main():
    system = System(
        config=Config()
    )

    system.start()
    ping = system.spawn(Ping)
    print('ping address is', ping)

    time.sleep(2)

    print('count:', system.ask(ping, 'count'))

    system.stop()


if __name__ == '__main__':
    main()
