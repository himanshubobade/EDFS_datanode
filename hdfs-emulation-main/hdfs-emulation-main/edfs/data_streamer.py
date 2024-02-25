import asyncio
import json

from edfs.block import Block, LocatedBlock
from edfs.config import *
from edfs.dfs_packet import DFSPacket
from edfs.utils import PacketUtils

class DataStreamer:

    def __init__(self):
        self.des_inode_id = None
        self.task = None
        self.data_queue = asyncio.Queue(MAX_QUEUE_SIZE)
        self.ack_queue = asyncio.Queue(MAX_QUEUE_SIZE)
        self.reader = None
        self.writer = None

    def setup(self, task, des_inode_id):
        self.task = task
        self.des_inode_id = des_inode_id

    async def enqueue(self, item):
       await self.data_queue.put(item)

    async def run(self):
        block_id = None
        blk_locs_info = None
        ack_task = None
        packets_buf = []
        num_bytes = 0
        while True:
            packet = await self.data_queue.get()
            num_bytes += packet.get_datalen()
            packets_buf.append(packet)
            await self.ack_queue.put(packet)
            if packet.is_last_packet_in_block():
                block_id, blk_locs_info = await self.request_new_block(num_bytes)
                target = blk_locs_info.pop(0)
                reader, writer = await self.setup_pipeline(block_id, target, blk_locs_info)
                ack_task = asyncio.create_task(self.recv_acks(reader))
                await self.writebock(writer, packets_buf, block_id, blk_locs_info)
                await self.wait_for_all_ack()
                print(f'DBG: block {block_id} was successfully sent to datanodes {target.get("name")} {" ".join([loc.get("name") for loc in blk_locs_info])}')
                ack_task.cancel()
                writer.close()
                packets_buf = []
                num_bytes = 0
            self.data_queue.task_done()

    async def recv_acks(self, nextnode_reader):
        buf = bytearray([])
        while True:
            data = await nextnode_reader.read(BUF_LEN)
            if not data:
                continue
            buf += data

            packets, ptr = PacketUtils.create_packets_from_buffer(buf)
            decoded_packets = [json.loads(packet.decode()) for packet in packets]
            seqnos = [packet.get("seqno") for packet in decoded_packets]
            for seqno in seqnos:
                packet = await self.ack_queue.get()
                print(f'DBG: received ack {seqno}, packet {packet.get_seqno()} popped from ack queue')
                self.ack_queue.task_done()
            buf = buf[ptr:]

    async def writebock(self, writer, packets_buf, block_id, next_datanodes):
        for packet in packets_buf:
            buf = {
                "seqno": packet.get_seqno(),
                "data": packet.get_data(),
                "is_last_packet_in_block": packet.is_last_packet_in_block(),
                "block_id": block_id,
                "next_datanodes": next_datanodes
            }
            writer.write(PacketUtils.encode(json.dumps(buf).encode()))
            await writer.drain()

    async def request_new_block(self, num_bytes):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        message = json.dumps({"cmd": CMD_ADD_BLOCK, "inode_id": self.des_inode_id, "num_bytes": num_bytes})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        block_id = response.get("block_id")
        blk_locs_info = response.get("blk_locs_info")

        writer.close()
        return block_id, blk_locs_info

    async def wait_for_all_ack(self):
        await self.ack_queue.join()

    async def finish(self):
        await self.ack_queue.join()
        await self.data_queue.join()
        if self.task:
            self.task.cancel()

    async def setup_pipeline(self, block_id, target, next_datanodes):
        reader, writer = await asyncio.open_connection(
            target.get("id"), target.get("port")
        )
        message = json.dumps({"cmd": CLI_DATANODE_CMD_SETUP_WRITE, "block_id": block_id, "next_datanodes": next_datanodes})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())

        return reader, writer

    def close(self):
        if self.writer:
            self.writer.close()
