import asyncio
import json

from edfs.config import *
from edfs.fs_data_input_stream import FSDataInputStream
from edfs.fs_data_output_stream import FSDataOutputStream

class DistributedFileSystem:
    def __init__(self):
         pass

    def close(self):
        pass

    async def open(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        message = json.dumps({"cmd": CMD_GET_BLOCK_LOCATIONS, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = bytearray([])
        while True:
            buf = await reader.read(BUF_LEN)
            if not buf:
                break
            data += buf
        writer.close()

        response = json.loads(data.decode())
        success = response.get("success")
        if success:
            block_locations = response.get("block_locations")
            return FSDataInputStream(block_locations)

        return None

    async def mkdir(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_MKDIR, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")

    async def rmdir(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_RMDIR, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if not success:
            print(response.get("msg"))

    async def create(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_CREATE, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        writer.close()

        response = json.loads(data.decode())
        success = response.get("success")
        if success:
            inode_id = response.get("inode_id")
            return FSDataOutputStream(inode_id)

        return None

    async def create_complete(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_CREATE_COMPLETE, "path": path})
        writer.write(message.encode())
        await writer.drain()

        writer.close()

    async def rm(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        request = json.dumps({"cmd": CMD_RM, "path": path})
        writer.write(request.encode())
        await writer.drain()

        writer.close()

    async def mv(self, src, des):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        request = json.dumps({"cmd": CMD_MV, "src": src, "des": des})
        writer.write(request.encode())
        await writer.drain()

        writer.close()

    async def exists(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_FILE_EXISTS, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        exists = response.get("exists")

        writer.close()
        return exists

    async def is_dir(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_IS_DIR, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        is_dir = response.get("is_dir")

        writer.close()
        return is_dir

    async def is_dir_empty(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_IS_DIR_EMPTY, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        is_dir_empty = response.get("is_dir_empty")

        writer.close()
        return is_dir_empty

    async def is_identical(self, path1, path2):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_IS_IDENTICAL, "path1": path1, "path2": path2})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())

        writer.close()
        return response.get("is_identical")

    async def is_root_dir(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_IS_ROOT_DIR, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        is_root = response.get("is_root")

        writer.close()
        return is_root
