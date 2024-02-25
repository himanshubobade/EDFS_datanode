from edfs.config import *

class Inode:
    def __init__(self, id, type, name, replication=0, preferredBlockSize=0, blocks=None):
        self.id = id
        self.type = type
        self.name = name
        self.replication = replication
        self.preferredBlockSize = preferredBlockSize
        self.blocks = blocks if blocks is not None else []
        self.dir_entries = [DirEnt(self, ".")]

    def is_dir(self):
        return self.type == DIR_TYPE

    def is_file(self):
        return self.type == FILE_TYPE

    def get_id(self):
        return self.id

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_replication(self):
        return self.replication

    def get_preferredBlockSize(self):
        return self.preferredBlockSize

    def get_blocks(self):
        return self.blocks

    def get_dirents(self):
        return self.dir_entries

    def set_name(self, name):
        self.name = name

    def add_dirent(self, inode, name):
        self.get_dirents().append(DirEnt(inode, name))

    def add_block(self, block_id):
        self.blocks.append(block_id)

    def get_parent_inode(self):
        for dirent in self.get_dirents():
            if dirent.name == "..":
                return dirent.inode

    def get_child_inode_by_name(self, name):
        for dirent in self.get_dirents():
            if dirent.name == name:
                return dirent.inode
        return None

    def get_children_ids(self):
        children_ids = []
        for dirent in self.get_dirents():
            if  dirent.name != "." and dirent.name != "..":
                children_ids.append(dirent.inode.get_id())
        return children_ids

    def get_path(self):
        cur = self
        parent = cur.get_parent_inode()
        path = [cur.get_name()]
        while parent != cur:
            cur = parent
            path.append(cur.get_name())
            parent = cur.get_parent_inode()
        return "/".join(path[::-1])

class DirEnt:
    def __init__(self, inode, name):
        self.inode = inode
        self.name = name
