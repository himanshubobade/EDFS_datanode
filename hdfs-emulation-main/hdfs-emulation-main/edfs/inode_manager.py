from edfs.config import *
from edfs.inode import Inode

class InodeManager():
    def __init__(self, fsimage, block_manager):
        self.bm = block_manager
        self.last_inode_id = INODE_ID_START
        self.last_block_id = BLOCK_ID_START
        self.id_to_inode = {}
        self.root_inode = None
        self.build_inodes(fsimage)

    def build_inodes(self, metadata):
        inodes = metadata["inodes"]
        directories = metadata["directories"]

        for inode in inodes:
            if inode["type"] == DIR_TYPE:
                node = Inode(inode["id"], inode["type"], inode["name"])
            else:
                blocks = [blk.get("id") for blk in inode.get("blocks")]
                node = Inode(inode["id"], inode["type"], inode["name"], inode["replication"], inode["preferredBlockSize"], blocks)
            self.id_to_inode[node.get_id()] = node
            if node.get_name() == ROOT_DIR_NAME:
                self.root_inode = node
            self.last_inode_id = max(self.last_inode_id, node.get_id())

        for directory in directories:
            parent_id = directory["parent"]
            children_ids = directory["children"]

            p_node = self.id_to_inode[parent_id]
            for child_id in children_ids:
                c_node = self.id_to_inode[child_id]
                p_node.add_dirent(c_node, c_node.get_name())
                c_node.add_dirent(p_node, "..")

        self.root_inode.add_dirent(self.root_inode, "..")

    def get_inode_by_id(self, id):
        return self.id_to_inode.get(id)

    def get_inode_from_path(self, path):
        path = path.strip(' /')
        path_list = path.split("/")

        cur_inode = self.root_inode
        if path_list[0] == "." :
            cur_inode = self.get_inode_from_path(DEFAULT_BASE_DIR)
            if cur_inode is None:
                return None
            path_list.pop(0)

        for s in path_list:
            if s == "": continue
            cur_inode = cur_inode.get_child_inode_by_name(s)
            if cur_inode is None:
                break
        return cur_inode

    def get_all_inodes(self):
        return list(self.id_to_inode.values())

    def get_all_inode_ids(self):
        return list(self.id_to_inode.keys())

    def get_baseinode_and_filename(self, path):
        path = path.strip(' /')
        path_list = path.split("/")

        filename = path_list.pop()
        base_dir_inode = self.get_inode_from_path("/".join(path_list))

        return (base_dir_inode, filename)

    def create_dir(self, base_inode, filename):
        new_dir_inode = Inode(self.last_inode_id + 1, DIR_TYPE, filename)
        base_inode.add_dirent(new_dir_inode, filename)
        new_dir_inode.add_dirent(base_inode, "..")
        self.id_to_inode[new_dir_inode.get_id()] = new_dir_inode
        self.last_inode_id += 1
        return new_dir_inode

    def remove_dir(self, parent, inode):
        dirents = parent.get_dirents()
        for i, dirent in enumerate(dirents):
            if dirent.name == inode.get_name():
                dirents.pop(i)
        del self.id_to_inode[inode.get_id()]

    def create_file(self, base_inode, filename):
        new_dir_inode = Inode(self.last_inode_id + 1, FILE_TYPE, filename, 3, DEFAULT_BLOCK_SZIE, None)
        base_inode.add_dirent(new_dir_inode, filename)
        new_dir_inode.add_dirent(base_inode, "..")
        self.id_to_inode[new_dir_inode.get_id()] = new_dir_inode
        self.last_inode_id += 1
        return new_dir_inode

    def rm(self, inode):
        parent = inode.get_parent_inode()
        for i, dirent in enumerate(parent.get_dirents()):
            if dirent.name == inode.name:
                parent.get_dirents().pop(i)
                break
        del self.id_to_inode[inode.get_id()]

        block_ids = inode.get_blocks()
        for block_id in block_ids:
            self.bm.delete_block(block_id)

    def move(self, src_inode, des_inode, name):
        parent = src_inode.get_parent_inode()
        for i, dirent in enumerate(parent.get_dirents()):
            if dirent.name == src_inode.get_name():
                parent.get_dirents().pop(i)
                break

        for dirent in src_inode.get_dirents():
            if dirent.name == "..":
                dirent.inode = des_inode
                break
        src_inode.set_name(name)
        des_inode.add_dirent(src_inode, name)

    def add_block_to(self, inode_id, block_id):
        inode = self.get_inode_by_id(inode_id)
        inode.add_block(block_id)

    def print_entries(self, dir_inode):
        dir_path = dir_inode.get_path()
        for dirent in  dir_inode.get_dirents():
            if dirent.name != "." and dirent.name != "..":
                print(f'{dir_path}/{dirent.name}')
        print()

    def print_recursive(self, inode, level):
        if inode.is_file():
            print(f'{"    " * level}{inode.get_name()}')
            return

        print(f'{"    " * level}{inode.get_name()}:')
        for dirent in inode.get_dirents():
            if dirent.name != "." and dirent.name != "..":
                self.print_recursive(dirent.inode, level + 1)
