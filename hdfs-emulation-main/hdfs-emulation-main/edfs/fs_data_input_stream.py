import asyncio
import json
import random

from edfs.config import *

class FSDataInputStream:
    def __init__(self, block_locations):
        self.block_locations = block_locations
        self.block_num = 0
        self.reader = None
        self.writer = None
        self.num_bytes = 0
        self.cur_num_bytes = 0
        # print(self.block_locations)
    def close(self):
        pass

    async def read(self, buf):
        if self.block_num >= len(self.block_locations):
            return -1

        if not self.writer:
            self.cur_num_bytes = 0
            block_id = self.block_locations[self.block_num].get("block_id")
            self.num_bytes = self.block_locations[self.block_num].get("num_bytes")
            locs = self.block_locations[self.block_num].get("locs")
            target_loc = random.choice(locs)
            self.reader, self.writer = await asyncio.open_connection(
                target_loc.get("ip"), target_loc.get("port")
            )

            await self.read_block(block_id, 0, self.num_bytes)

        data = await self.reader.read(BUF_LEN)
        self.cur_num_bytes += len(data)
        if self.cur_num_bytes == self.num_bytes:
            self.writer.close()
            self.writer = None
            self.block_num += 1

        buf += data
        return len(data)

    async def read_block(self, block_id, offset, num_bytes):
        request = {"cmd": CLI_DATANODE_CMD_READ, "block_id": block_id, "offset": offset, "num_bytes": num_bytes}
        self.writer.write(json.dumps(request).encode())
        await self.writer.drain()
