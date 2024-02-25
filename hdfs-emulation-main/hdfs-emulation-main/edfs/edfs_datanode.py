import asyncio
import json
import os

from edfs.block_manager import BlockManager
from edfs.config import *
from edfs.dfs_packet import DFSPacket
from edfs.utils import PacketUtils

class EDFSDataNode:
    @classmethod
    async def create_instance(cls, ip, port, name):
        self = EDFSDataNode(ip, port, name)
        if not os.path.exists(DATANODE_DATA_DIR):
            os.makedirs(DATANODE_DATA_DIR)

        if not os.path.exists(f'{DATANODE_DATA_DIR}/{self.name}'):
            os.makedirs(f'{DATANODE_DATA_DIR}/{self.name}')
        return self

    def __init__(self, ip, port, name):
        self.ip = ip
        self.port = port
        self.name = name

    async def register(self):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        message = json.dumps({
            "cmd": DN_CMD_REGISTER,
            "ip": self.ip,
            "port": self.port,
            "name": self.name,
            "blocks": self.get_all_block_ids()
        })
        writer.write(message.encode())
        await writer.drain()
        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if success:
            print(f'DBG: successfully registered and sent the block report to the namenode')

    async def serve(self):
        print(f'DBG: Datanode {self.name} starts serving at {self.ip}:{self.port}')

        server = await asyncio.start_server(
        self.handle_client, self.ip, self.port)

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        data = await reader.read(BUF_LEN)
        request = json.loads(data.decode())
        command = request.get("cmd")
        if command == CLI_DATANODE_CMD_SETUP_WRITE:
            block_id, nextnode_reader, nextnode_writer = await self.setup_write_pipeline(reader, writer, request)
            end_of_pipeline = nextnode_writer is None
            tasks = asyncio.gather(
                self.recv_and_write(reader, writer, nextnode_reader, nextnode_writer, block_id, end_of_pipeline),
                self.recv_acks(writer, nextnode_reader, end_of_pipeline)
            )
            await tasks

            if nextnode_writer:
                nextnode_writer.close()

        elif command == CLI_DATANODE_CMD_READ:
            block_id, offset, num_bytes = request.get("block_id"), request.get("offset"), request.get("num_bytes")
            await self.read_block(writer, block_id, offset, num_bytes)

        writer.close()

    async def setup_write_pipeline(self, reader, writer, request):
        block_id = request.get("block_id")
        next_datanodes = request.get("next_datanodes")
        nextnode_reader, nextnode_writer = None, None
        if next_datanodes:
            target = next_datanodes.pop(0)
            nextnode_reader, nextnode_writer = await asyncio.open_connection(
                target.get("id"), target.get("port")
            )
            message = json.dumps({"cmd": CLI_DATANODE_CMD_SETUP_WRITE, "block_id": block_id, "next_datanodes": next_datanodes})
            nextnode_writer.write(message.encode())
            await nextnode_writer.drain()

            nextnode_data = await nextnode_reader.read(BUF_LEN)
            nextnode_response = json.loads(nextnode_data.decode())

        response = {"success": True}
        writer.write(json.dumps(response).encode())
        await writer.drain()
        print(f'DBG: successfully setup the write pipeline, waiting for packets')
        return block_id, nextnode_reader, nextnode_writer

    async def recv_and_write(self, prevnode_reader, prevnode_writer, nextnode_reader, nextnode_writer, block_id, end_of_pipeline):
        buf = bytearray([])
        block_data = []
        is_last_packet = False
        while True:
            data = await prevnode_reader.read(BUF_LEN)

            if not end_of_pipeline:
                nextnode_writer.write(data)
                await nextnode_writer.drain()

            buf += data

            packets, ptr = PacketUtils.create_packets_from_buffer(buf)
            decoded_packets = [json.loads(packet.decode()) for packet in packets]
            for decoded_packet in decoded_packets:
                block_data.append(decoded_packet.get("data"))
                if decoded_packet.get("is_last_packet_in_block"):
                    is_last_packet = True
            if end_of_pipeline:
                seqnos = [packet.get("seqno") for packet in decoded_packets]
                await self.send_acks(prevnode_writer, seqnos)
            buf = buf[ptr:]

            if is_last_packet:
                break

        with open(f'{DATANODE_DATA_DIR}/{self.name}/{BlockManager.get_filename_from_block_id(block_id)}', 'w') as f:
            f.write("".join(block_data))

    async def recv_acks(self, prevnode_writer, nextnode_reader, end_of_pipeline):
        if end_of_pipeline: return

        buf = bytearray([])
        while True:
            data = await nextnode_reader.read(BUF_LEN)
            if not data:
                break
            buf += data

            packets, ptr = PacketUtils.create_packets_from_buffer(buf)
            decoded_packets = [json.loads(packet.decode()) for packet in packets]
            seqnos = [packet.get("seqno") for packet in decoded_packets]
            await self.send_acks(prevnode_writer, seqnos)
            buf = buf[ptr:]

    async def send_acks(self, prevnode_writer, seqnos):
        for seqno in seqnos:
            ack = {
                "type": "ack",
                "seqno": seqno
            }
            prevnode_writer.write(PacketUtils.encode(json.dumps(ack).encode()))
            await prevnode_writer.drain()

    async def read_block(self, writer, block_id, offset, num_bytes):
        filename = BlockManager.get_filename_from_block_id(block_id)
        print(f'DBG: client requested to read block {block_id} for {num_bytes} bytes from offset {offset}')
        if not os.path.exists(f'{DATANODE_DATA_DIR}/{self.name}/{filename}'):
            print(f'DBG: block {block_id} does not exist')
            return
        with open(f'{DATANODE_DATA_DIR}/{self.name}/{filename}', 'r') as f:
            f.seek(offset)
            cur_num_bytes = 0
            while True:
                data = f.read(min(DEFAULT_PACKET_DATA_SIZE, num_bytes - cur_num_bytes))
                if not data:
                    break
                writer.write(data.encode())
                await writer.drain()
                cur_num_bytes += len(data)


    def get_all_block_ids(self):
        return [BlockManager.get_file_block_id(filename) for filename in os.listdir(f'{DATANODE_DATA_DIR}/{self.name}') if filename.startswith(BLOCK_PREFIX)]