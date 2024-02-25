from edfs.config import *


class Block:
    def __init__(self, block_id, inode_id, num_bytes, locations=None):
        self.id = block_id
        self.inode_id = inode_id
        self.num_bytes = num_bytes
        self.locs = locations if locations != None else []

    def get_id(self):
        return self.id

    def get_inode_id(self):
        return self.inode_id

    def get_num_bytes(self):
        return self.num_bytes

    def get_locs(self):
        return self.locs

    def get_filename(self):
        return f'{BLOCK_PREFIX}{"0" * (20 - len(str(self.id)))}{self.id}'

    def add_loc(self, datanonde_id):
        self.locs.append(datanonde_id)


class LocatedBlock:
    def __init__(self, block_id, blk_locs_info):
        self.id = block_id
        self.blk_locs_info = blk_locs_info

    def __str__(self):
        s = f'Block Id: {self.id}\nDataNodes in the pipeline:\n'
        for blk_loc in self.blk_locs_info:
            s += f'DataNode {blk_loc.get("id")} ({blk_loc.get("name")}): {blk_loc.get("ip")}:{blk_loc.get("port")}\n'
        s += "\n"
        return s
