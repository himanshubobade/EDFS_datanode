from collections import deque
from edfs.block import Block
from edfs.config import *


class BlockManager:
    @staticmethod
    def get_file_block_id(filename):
        return int(filename.replace(BLOCK_PREFIX, ""))

    @staticmethod
    def get_filename_from_block_id(block_id):
        return f'{BLOCK_PREFIX}{"0" * (20 - len(str(block_id)))}{block_id}'

    def __init__(self, fsimage):
        self.id_to_block = {}
        self.last_block_id = BLOCK_ID_START
        self.free_block_ids = deque([])
        self.build_blocks(fsimage)

    def build_blocks(self, fsimage):
        inodes = fsimage.get("inodes")
        for inode in inodes:
            inode_id = inode.get("id")
            blocks = inode.get("blocks")
            if not blocks: continue
            for block in blocks:
                blk = Block(block.get("id"), inode_id, block.get("numBytes"), [])
                self.register_block(blk)
        free_blocks = fsimage.get("freeBlocks")
        self.free_block_ids += free_blocks
        print(f'DBG: free block ids: {self.get_free_block_ids()}')

    def register_block(self, block):
        self.id_to_block[block.get_id()] = block
        self.last_block_id = max(self.last_block_id, block.get_id())

    def get_block_by_id(self, id):
        return self.id_to_block.get(id)

    def add_block_loc(self, id, datanode_id):
        block = self.get_block_by_id(id)
        if not block:
            return
        block.add_loc(datanode_id)

    def allocate_block_for(self, inode_id, num_bytes):
        if self.free_block_ids:
            block_id = self.free_block_ids.popleft()
        else:
            self.last_block_id += 1
            block_id = self.last_block_id
        blk = Block(block_id, inode_id, num_bytes, [])
        self.register_block(blk)
        return blk

    def delete_block(self, block_id):
        if block_id not in self.id_to_block:
            return
        del self.id_to_block[block_id]
        self.free_block_ids.append(block_id)

    def get_free_block_ids(self):
        return sorted(list(self.free_block_ids))
