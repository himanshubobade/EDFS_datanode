import asyncio
import json
import os
import random

from edfs.block_manager import BlockManager
from edfs.config import *
from edfs.datanode_info import DataNodeInfo, DataNodeManager
from edfs.editlog_manager import EditLogManager
from edfs.inode_manager import InodeManager


class EDFSNameNode:
    def __init__(self):
        fsimage = self.read_fsimage()
        self.bm = BlockManager(fsimage)
        self.im = InodeManager(fsimage, self.bm)
        self.dnm = DataNodeManager()
        self.elm = EditLogManager(self.im, self.bm)
        self.take_snapshot()

    async def handle_client(self, reader, writer):
        data = await reader.read(BUF_LEN)
        if not data:
            writer.close()
            return

        request = json.loads(data.decode())
        command = request.get("cmd")
        if command == CMD_LS:
            await self.ls(writer, request.get("path"))
        elif command == CMD_MKDIR:
            await self.mkdir(writer, request.get("path"))
        elif command == CMD_RMDIR:
            await self.rmdir(writer, request.get("path"))
        elif command == CMD_CREATE:
            await self.create(reader, writer, request)
        elif command == CMD_RM:
            await self.rm(request)
        elif command == CMD_MV:
            await self.mv(request)
        elif command == CMD_TREE:
            await self.tree(writer,request.get("path"))
        elif command == DN_CMD_REGISTER:
            await self.register_datanode(writer, request)
        elif command == CMD_ADD_BLOCK:
            await self.add_block(writer, request)
        elif command == CMD_GET_BLOCK_LOCATIONS:
            await self.get_block_locations(writer, request)
        elif command == CMD_CREATE_COMPLETE:
            await self.create_complete(request)
        elif command == CMD_FILE_EXISTS:
            await self.exists(writer, request)
        elif command == CMD_IS_DIR:
            await self.is_dir(writer, request)
        elif command == CMD_IS_IDENTICAL:
            await self.is_identical(writer, request)
        elif command == CMD_IS_ROOT_DIR:
            await self.is_root_dir(writer, request)
        elif command == CMD_IS_DIR_EMPTY:
            await self.is_dir_empty(writer, request)

        writer.close()

    async def ls(self, writer, path):
        inode = self.im.get_inode_from_path(path)
        if inode is None:
            response = {"success": False, "msg": f'ls: {path}: No such file or directory'}
            writer.write(json.dumps(response).encode())
            return

        entries = []
        if inode.is_file():
            entries.append(inode.get_path())
        else:
            for dirent in inode.get_dirents():
                if dirent.name != "." and dirent.name != "..":
                    entries.append(dirent.inode.get_path())

        response = {"success": True, "entries": entries}
        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def mkdir(self, writer, path):
        base_dir_inode, filename = self.im.get_baseinode_and_filename(path)

        new_dir_inode = self.im.create_dir(base_dir_inode, filename)
        log = {"edit_type": EDIT_TYPE_MKDIR, "parent": base_dir_inode.get_id(), "name": new_dir_inode.get_name()}
        self.elm.write_log(log)
        response = {"success": True, "msg": f'mkdir: {path}: successfully created with inode number {new_dir_inode.get_id()}'}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def rmdir(self, writer, path):
        inode = self.im.get_inode_from_path(path)
        parent = inode.get_parent_inode()
        self.im.remove_dir(parent, inode)
        log = {"edit_type": EDIT_TYPE_RMDIR, "parent": parent.get_id(), "remove": inode.get_id(), "name": inode.get_name()}
        self.elm.write_log(log)
        response = {"success": True, "msg": f'rmdir: {path}: successfully removed the directory'}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def create(self, reader, writer, request):
        path = request.get("path")
        base_dir_inode, filename = self.im.get_baseinode_and_filename(path)
        if base_dir_inode is None:
            response = {"success": False, "error": ERR_FILE_NOT_FOUND}
        elif filename == "" or base_dir_inode.get_child_inode_by_name(filename) != None:
            response = {"success": False, "error": ERR_FILE_EXIST}
        else:
            new_file_inode = self.im.create_file(base_dir_inode, filename)
            log = {"edit_type": EDIT_TYPE_CREATE, "parent": base_dir_inode.get_id(), "name": new_file_inode.get_name()}
            self.elm.write_log(log)
            response = {"success": True, "inode_id": new_file_inode.get_id()}

        writer.write(json.dumps(response).encode())
        await writer.drain()

        return

    async def create_complete(self, request):
        path = request.get("path")
        print(f'DBG: Finish creating {path}')

    async def rm(self, request):
        path = request.get("path")
        inode = self.im.get_inode_from_path(path)
        self.im.rm(inode)

        log = {"edit_type": EDIT_TYPE_RM, "inode_id": inode.get_id()}
        self.elm.write_log(log)

        print(f'DBG: {inode.get_path()} is successfully removed')

    async def mv(self, request):
        src, des = request.get("src"), request.get("des")
        src_inode = self.im.get_inode_from_path(src)
        des_dir_inode, filename = self.im.get_baseinode_and_filename(des)

        self.im.move(src_inode, des_dir_inode, filename)

        log = {"edit_type": EDIT_TYPE_MV, "src_inode_id": src_inode.get_id(), "des_inode_id": des_dir_inode.get_id(), "name": filename}
        self.elm.write_log(log)

        print(f'DBG: {src} is successfully moved to {des}')

    async def is_root_dir(self, writer, request):
        path = request.get("path")
        inode = self.im.get_inode_from_path(path)
        if inode == self.im.root_inode:
            response = {"is_root": True}
        else:
            response = {"is_root": False}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def exists(self, writer, request):
        path = request.get("path")
        inode = self.im.get_inode_from_path(path)
        if inode is None:
            response = {"exists": False}
        else:
            response = {"exists": True}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def is_dir(self, writer, request):
        path = request.get("path")
        inode = self.im.get_inode_from_path(path)
        if inode is None or inode.is_file():
            response = {"is_dir": False}
        else:
            response = {"is_dir": True}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def is_dir_empty(self, writer, request):
        path = request.get("path")
        inode = self.im.get_inode_from_path(path)
        if inode is None or inode.is_file() or len(inode.get_dirents()) > 2:
            response = {"is_dir_empty": False}
        else:
            response = {"is_dir_empty": True}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def is_identical(self, writer, request):
        path1, path2 = request.get("path1"), request.get("path2")
        inode1 = self.im.get_inode_from_path(path1)
        inode2 = self.im.get_inode_from_path(path2)

        if inode1 is None or inode2 is None or inode1.get_id() != inode2.get_id():
            response = {"is_identical": False}
        else:
            response = {"is_identical": True}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def tree(self, writer, path):
        inode = self.im.get_inode_from_path(path)
        if inode is None:
            response = {"success": False, "msg": f'tree: {path}: No such file or directory'}
            writer.write(json.dumps(response).encode())
            return
        response = {"success": True, "files": self.get_all_files(inode)}
        writer.write(json.dumps(response).encode())
        await writer.drain()

    def get_all_files(self, inode):
        children = []
        for dirent in inode.get_dirents():
            if dirent.name != "." and dirent.name != "..":
                children.append(self.get_all_files(dirent.inode))
        return {"name": inode.get_name(), "type": inode.get_type(), "path": inode.get_path(), "children": children}

    def read_fsimage(self):
        if not os.path.exists(NAMENODE_METADATA_DIR):
            os.makedirs(NAMENODE_METADATA_DIR)

        if not os.path.exists(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}'):
            with open(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}', 'w') as f:
                fsimage = {"inodes": [{"id": INODE_ID_START, "type": "DIRECTORY", "name": ""}], "directories": [], "freeBlocks": []}
                json.dump(fsimage, f)
        else:
            with open(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}', 'r') as f:
                fsimage = json.load(f)

        return fsimage

    def take_snapshot(self):
        self.elm.process_edit_logs()

        inodes = []
        directories = []
        for inode in self.im.get_all_inodes():
            if inode.is_file():
                blocks = []
                for block_id in inode.get_blocks():
                    blk = self.bm.get_block_by_id(block_id)
                    blocks.append({"id":  blk.get_id(), "numBytes": blk.get_num_bytes()})

                inodes.append({
                    "id": inode.get_id(),
                    "type": inode.get_type(),
                    "name":  inode.get_name(),
                    "replication": inode.get_replication(),
                    "preferredBlockSize": inode.get_preferredBlockSize(),
                    "blocks": blocks
                })
            else:
                inodes.append({
                    "id": inode.get_id(),
                    "type": inode.get_type(),
                    "name":  inode.get_name(),
                })

                children_ids = inode.get_children_ids()
                if children_ids:
                    directories.append({
                        "parent": inode.get_id(),
                        "children": children_ids
                    })
        fsimage = {"inodes": inodes, "directories": directories, "freeBlocks": self.bm.get_free_block_ids()}
        with open(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}', 'w') as f:
            json.dump(fsimage, f, indent=2)

        self.elm.remove_edit_logs()

    async def register_datanode(self, writer, request):
        datanode_info = DataNodeInfo(request.get("ip"), request.get("port"), request.get("name"))
        self.dnm.register(datanode_info)
        blocks = request.get("blocks")
        for block_id in blocks:
            self.bm.add_block_loc(block_id, datanode_info.get_id())
        response = {
            "success": True,
            "msg": f'Datanode {datanode_info.get_id()} ({datanode_info.get_name()}): {datanode_info.get_ip()}:{datanode_info.get_port()}'
        }
        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def add_block(self, writer, request):
        inode_id = request.get("inode_id")
        num_bytes = request.get("num_bytes")
        blk = self.bm.allocate_block_for(inode_id, num_bytes)
        self.im.add_block_to(inode_id, blk.get_id())
        blk_locs = self.select_block_locs(REPLICATION_FACTOR)
        blk_locs_info = []
        for datanode_info in blk_locs:
            blk_locs_info.append(datanode_info.get_info())
            blk.add_loc(datanode_info.get_id())

        log = {"edit_type": EDIT_TYPE_ADD_BLOCK, "inode_id": inode_id, "block_id": blk.get_id(), "num_bytes": blk.get_num_bytes()}
        self.elm.write_log(log)
        response = {"success": True, "inode_id": inode_id, "block_id": blk.get_id(), "blk_locs_info": blk_locs_info}

        writer.write(json.dumps(response).encode())
        await writer.drain()

        print(f'DBG: client request to add block {blk.get_id()} ({blk.get_num_bytes()} bytes)')
        return

    # TODO: for simplicity, we randomly choose from all datanodes
    def select_block_locs(self, num):
        datanodes = self.dnm.get_all_datanodes()
        return random.sample(datanodes, num)

    async def get_block_locations(self, writer, request):
        path = request.get("path")
        inode = self.im.get_inode_from_path(path)
        if inode is None:
            response = {"success": False, "error": ERR_FILE_NOT_FOUND}
        elif inode.is_dir():
            response = {"success": False, "error": ERR_IS_DIR}
        else:
            block_ids = inode.get_blocks()
            block_locations = []
            for block_id in block_ids:
                locs = []
                blk = self.bm.get_block_by_id(block_id)
                datanode_ids = blk.get_locs()
                for datanode_id in datanode_ids:
                    datanode_info = self.dnm.get_datanode_by_id(datanode_id)
                    locs.append(datanode_info.get_info())
                block_locations.append({"block_id": block_id, "num_bytes": blk.get_num_bytes(), "locs": locs})

            response = {"success": True, "block_locations": block_locations}
        writer.write(json.dumps(response).encode())
        await writer.drain()
