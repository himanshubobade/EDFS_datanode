import asyncio
import json
import os
import sys

from edfs.config import *
from edfs.distributed_file_system import DistributedFileSystem
from edfs.fs_data_output_stream import FSDataOutputStream


class EDFSClient:
    @classmethod
    async def create(cls):
        self = EDFSClient()
        self.namenode_reader, self.namenode_writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        return self

    def __init__(self):
        self.dfs = DistributedFileSystem()

    def close(self):
        if self.namenode_writer:
            self.namenode_writer.close()
        self.dfs.close()

    async def ls(self, path):
        message = json.dumps({"cmd": CMD_LS, "path": path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await  self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if not success:
            print(response.get("msg"))
        else:
            entries = response.get("entries")
            if not entries:
                return

            print(f'Found {len(entries)} items')
            for ent in entries:
                print(ent)

    async def mkdir(self, path):
        if await self.dfs.exists(path):
            print(f'mkdir: {path}: File exists')
            return
        elif not await self.dfs.exists(os.path.dirname(path.strip(" /"))):
            print(f'mkdir: {os.path.dirname(path.strip(" /"))}: No such file or directory')
            return

        await self.dfs.mkdir(path)
        self.dfs.close()

    async def rmdir(self, path):
        if await self.dfs.is_root_dir(path):
            print(f'rmdir: Can not remvoe the root directory')
            return
        elif not await self.dfs.exists(path):
            print(f'rmdir: {path}: No such file or directory')
            return
        elif not await self.dfs.is_dir(path):
            print(f'rmdir: {path}: Is not a directory')
            return
        elif not await self.dfs.is_dir_empty(path):
            print(f'rmdir: {path}: Directory is not empty')
            return

        await self.dfs.rmdir(path)
        self.dfs.close()

    async def touch(self, path):
        if not await self.dfs.exists(os.path.dirname(path)):
            print(f'touch: {os.path.dirname(path)}: No such file or directory')
            return
        elif await self.dfs.is_dir(path):
            print(f'touch: {path}: Is a directory')
            return
        elif await self.dfs.exists(path):
            return

        out_stream = await self.dfs.create(path)
        await out_stream.close()
        self.dfs.close()

    async def rm(self, path):
        if not await self.dfs.exists(path):
            print(f'rm: {path}: No such file or directory')
            return
        elif await self.dfs.is_dir(path):
            print(f'rm: {path}: Is a directory')
            return
        await self.dfs.rm(path)

    async def cat(self, path):
        if not await self.dfs.exists(path):
            print(f'cat: {path}: No such file or directory')
            return
        if await self.dfs.is_dir(path):
            print(f'cat: {path}: Is a directory')
            return

        in_stream = await self.dfs.open(path)
        if not in_stream:
            return
        buf = bytearray([])
        while (await in_stream.read(buf)) >= 0:
            if len(buf) >= BUF_LEN:
                print(buf.decode(), end="")
                buf = bytearray([])
        print(buf.decode(), end="")

        in_stream.close()
        self.dfs.close()

    # TODO: currently only support file types
    # Should implement recursive put all files in a directory in the future
    async def put(self, local_path, remote_path):
        if os.path.basename(remote_path) == "" or await self.dfs.is_dir(remote_path):
            target_path = f'{remote_path}/{os.path.basename(local_path)}'
        else:
            target_path = remote_path

        if not os.path.exists(local_path):
            print(f'put: {local_path}: No such file or directory')
            return False
        elif await self.dfs.exists(target_path):
            print(f'put: {target_path}: File exists')
            return False
        elif not await self.dfs.exists(os.path.dirname(remote_path)):
            print(f'put: {os.path.dirname(remote_path)}: No such file or directory: hdfs://localhost:9000{os.path.dirname(remote_path)}')
            return False
        elif not await self.dfs.is_dir(os.path.dirname(remote_path)):
            print(f'put: {os.path.dirname(remote_path)} (is not a directory)')
            return False

        out_stream = await self.dfs.create(target_path)
        if not out_stream:
            return False
        await out_stream.write(local_path)
        await self.dfs.create_complete(target_path)
        await out_stream.close()
        self.dfs.close()

        return True

    # TODO: currently only support file types
    # Should implement recursive get all files in a directory in the future
    async def get(self, remote_path, local_path):
        if os.path.exists(local_path):
            print(f'get: {local_path}: File exists')
            return
        if not await self.dfs.exists(remote_path):
            print(f'get: {remote_path}: No such file or directory')
            return

        in_stream = await self.dfs.open(remote_path)
        if not in_stream:
            return

        with open(local_path, 'a') as f:
            buf = bytearray([])
            while (await in_stream.read(buf)) > 0:
                if len(buf) >= BUF_LEN:
                    f.write(buf.decode())
                    buf = bytearray([])
            f.write(buf.decode())

        in_stream.close()
        self.dfs.close()

    async def get_file(self, path):
        if not await self.dfs.exists(path):
            return {"success": False}
        if await self.dfs.is_dir(path):
            return {"success": False}

        in_stream = await self.dfs.open(path)
        if not in_stream:
            return {"success": False}

        buf = bytearray([])
        while (await in_stream.read(buf)) > 0:
            continue

        in_stream.close()
        self.dfs.close()

        return {"success": True, "file": buf.decode()}

    async def mv(self, src, des):
        if not await self.dfs.exists(src):
            print(f'mv: {src}: No such file or directory')
            return

        if not await self.dfs.exists(des):
            dirname = os.path.dirname(des.rstrip("/"))
            if not await self.dfs.exists(dirname) or not await self.dfs.is_dir(dirname):
                print(f'mv: {dirname}: No such file or directory: edfs://localhost:9000{dirname}')
                return
            target = des
        else:
            if await self.dfs.is_dir(des):
                target = f'{des}/{os.path.basename(src.rstrip("/"))}'
                if await self.dfs.is_identical(src, target):
                    print(f'mv: {src} to edfs://localhost:9000{target}: are identical')
                    return
                elif await self.dfs.exists(target):
                    print(f'mv: {target}: File exists')
                    return
                elif await self.dfs.is_identical(src, des):
                    print(f'mv: {src} to edfs://localhost:9000{des}: is a subdirectory of itself')
                    return
            else:
                print(f'mv: {des}: File exists')
                return

        await self.dfs.mv(src, target)

    async def tree(self, path):
        message = json.dumps({"cmd": CMD_TREE, "path": path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if not success:
            print(response.get("msg"))
        else:
            root = response.get("files")
            output = []
            self.tree_helper(root, 0, output)
            print("\n".join(output))

    def tree_helper(self, file, level, output):
        _name = file.get("name")
        _type = file.get("type")

        if _type == FILE_TYPE:
            output.append(f'{"    " * level}{_name}')
        else:
            output.append(f'{"    " * level}{_name}:')
            children = file.get("children")
            for child in children:
                self.tree_helper(child, level + 1, output)

    async def get_all_files(self):
        message = json.dumps({"cmd": CMD_TREE, "path": "/"})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        return response.get("files")
