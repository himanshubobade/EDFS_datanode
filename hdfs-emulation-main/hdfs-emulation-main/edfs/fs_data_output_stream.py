import asyncio

from collections import deque
from edfs.config import *
from edfs.data_streamer import DataStreamer
from edfs.dfs_packet import DFSPacket

class FSDataOutputStream:
    def __init__(self, des_inode_id):
        self.streamer = DataStreamer()
        self.task = None
        self.des_inode_id = des_inode_id

    def get_streamer(self):
        return self.streamer

    async def enqueue_packet(self, packet):
        await self.get_streamer().enqueue(packet)

    async def wait_for_streamer(self):
        await self.get_streamer().finish()

    async def write(self, src):
        self.task = asyncio.create_task(self.streamer.run())
        self.get_streamer().setup(self.task, self.des_inode_id)
        block_capacity = DEFAULT_BLOCK_SZIE
        with open(src, 'r') as f:
            data = f.read(min(DEFAULT_PACKET_DATA_SIZE, block_capacity))
            while len(data) > 0:
                block_capacity -= len(data)
                next_block_capacity = DEFAULT_BLOCK_SZIE if block_capacity == 0 else block_capacity
                next_data = f.read(min(DEFAULT_PACKET_DATA_SIZE, next_block_capacity))
                if block_capacity == 0 or len(next_data) == 0:
                    packet = DFSPacket.create_packet(data, True)
                    block_capacity = DEFAULT_BLOCK_SZIE
                else:
                    packet = DFSPacket.create_packet(data, False)
                await self.enqueue_packet(packet)
                data = next_data
                block_capacity = next_block_capacity

        await self.wait_for_streamer()


    async def close(self):
        self.streamer.close()
