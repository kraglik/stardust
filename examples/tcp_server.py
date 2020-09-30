import time
import asyncio

from stardust import Actor, System, Config
from stardust.messages import WakeUp


class Connection(Actor):
    def __init__(self, reader, writer, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reader = reader
        self.writer = writer

    async def receive(self, message, sender):
        while True:
            data = await self.reader.read(100)
            message = data.decode()

            print(message, message == 'q\n')
            if message == 'q\n':
                self.writer.write("bye!\n")
                await self.writer.drain()
                self.writer.close()
                return

            addr = self.writer.get_extra_info('peername')
            print("Received %r from %r at %r" % (message, addr, self.ref()))
            self.writer.write(data)

            await self.writer.drain()


class Server(Actor):
    async def receive(self, message, sender):
        await asyncio.start_server(self.spawn_connection, '127.0.0.1', 8888)

    async def spawn_connection(self, reader, writer):
        print('spawning...')
        connection = await self.spawn(Connection, reader, writer)
        print('spawned connection at', connection)


def main():
    system = System(
        config=Config()
    )

    system.start()
    server = system.spawn(Server)
    print('server address is', server)

    try:
        while True:
            time.sleep(0.05)
    except KeyboardInterrupt:
        system.stop()


if __name__ == '__main__':
    main()
