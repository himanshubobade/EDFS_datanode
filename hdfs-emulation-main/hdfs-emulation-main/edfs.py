# more details at:
#   https://docs.python.org/3/library/asyncio-stream.html#examples

import asyncio
import json
import os
import sys

from edfs.config import *
from edfs.edfs_client import EDFSClient

async def main():
    if (len(sys.argv) < 2):
        print("Please provide a edfs command!")
        exit(-1)

    edfs_client = await EDFSClient.create()

    command = sys.argv[1]
    if command == CLI_LS:
        path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_BASE_DIR
        await edfs_client.ls(path)
    elif command == CLI_MKDIR:
        if len(sys.argv) < 3:
            print("-mkdir: Not enough arguments: expected 1 but got 0")
            exit(-1)
        path = sys.argv[2]
        await edfs_client.mkdir(path)
    elif command == CLI_RMDIR:
        if len(sys.argv) < 3:
            print("-rmdir: Not enough arguments: expected 1 but got 0")
            exit(-1)
        path = sys.argv[2]
        await edfs_client.rmdir(path)
    elif command == CLI_TOUCH:
        if len(sys.argv) < 3:
            print("-touch: Not enough arguments: expected 1 but got 0")
            exit(-1)
        await edfs_client.touch(sys.argv[2])
    elif command == CLI_RM:
        if len(sys.argv) < 3:
            print("-rm: Not enough arguments: expected 1 but got 0")
            exit(-1)
        await edfs_client.rm(sys.argv[2])
    elif command == CLI_CAT:
        if len(sys.argv) < 3:
            print("-cat: Not enough arguments: expected 1 but got 0")
            exit(-1)
        path = sys.argv[2]
        await edfs_client.cat(path)
    elif command == CLI_PUT:
        if len(sys.argv) < 3:
            print("-put: Not enough arguments: expected 1 but got 0")
            exit(-1)
        local_path = sys.argv[2]
        remote_path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_BASE_DIR
        local_path = local_path.rstrip("/")
        await edfs_client.put(local_path, remote_path)
    elif command == CLI_GET:
        if len(sys.argv) < 3:
            print("-get: Not enough arguments: expected 1 but got 0")
            exit(-1)
        remote_path = sys.argv[2]
        local_path = sys.argv[3] if len(sys.argv) > 3 else f'./{os.path.basename(remote_path.lstrip("/"))}'
        await edfs_client.get(remote_path, local_path)
    elif command == CLI_MV:
        if len(sys.argv) < 4:
            print(f'-mv: Not enough arguments: expected 2 but got {len(sys.argv) - 2}')
            exit(-1)
        await edfs_client.mv(sys.argv[2], sys.argv[3])
    elif command == CLI_TREE:
        path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_BASE_DIR
        await edfs_client.tree(path)
    else:
        print(f'Command not found: {command}')
        exit(-1)

    edfs_client.close()


if __name__ == "__main__":
    asyncio.run(main())
