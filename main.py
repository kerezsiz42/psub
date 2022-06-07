import signal
import uuid
from uuid import UUID
import asyncio
from asyncio import StreamReader, StreamWriter
from typing import Dict, Tuple, List, Any


class Client:
    def __init__(self, writer: StreamWriter):
        self._id: UUID = uuid.uuid4()
        self._writer: StreamWriter = writer

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def writer(self) -> StreamWriter:
        return self._writer

    def __eq__(self, client: Any) -> bool:
        return bool(self._id == client.id)


channels_to_clients: Dict[str, List[Client]] = dict()


def is_subscribed(channel: str, client: Client) -> bool:
    if not channel_exists(channel):
        return False
    clients = channels_to_clients.get(channel) or list()
    if client in clients:
        return True
    return False


def channel_exists(channel: str) -> bool:
    return channel in channels_to_clients.keys()


def subscribe(channel: str, client: Client) -> None:
    if is_subscribed(channel, client):
        return
    if not channel_exists(channel):
        channels_to_clients[channel] = list()
    channels_to_clients[channel].append(client)


def unsubscribe(channel: str, client: Client) -> None:
    if not channel_exists(channel):
        return
    if not is_subscribed(channel, client):
        return
    channels_to_clients[channel].remove(client)
    if len(channels_to_clients[channel]) == 0:
        channels_to_clients.pop(channel, None)


async def publish(channel: str, message: str) -> None:
    if not channel_exists(channel):
        return
    coros = list()
    for client in channels_to_clients[channel]:
        client.writer.write(f'{message}\n'.encode())
        coros.append(client.writer.drain())
    await asyncio.gather(*coros)


async def handle(reader: StreamReader, writer: StreamWriter) -> None:
    client = Client(writer)
    while True:
        line = await reader.readline()
        if reader.at_eof():
            print(f'client {client.id} closed connection')
            break
        data = line.decode().rstrip().split()

        if len(data) == 0:
            continue

        if data[0] == 'subscribe':
            try:
                channel = data[1]
            except IndexError:
                continue
            subscribe(channel, client)
        elif data[0] == 'unsubscribe':
            try:
                channel = data[1]
            except IndexError:
                continue
            unsubscribe(channel, client)
        elif data[0] == 'publish':
            try:
                channel = data[1]
                message = data[2]
            except IndexError:
                continue
            await publish(channel, message)


async def main_coro() -> None:
    try:
        server = await asyncio.start_server(handle, host='localhost', port=8080)
        await server.start_serving()
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        server.close()
        await server.wait_closed()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    main_task = loop.create_task(main_coro())
    loop.add_signal_handler(signal.SIGTERM, main_task.cancel)
    loop.add_signal_handler(signal.SIGINT, main_task.cancel)
    loop.run_until_complete(main_task)
