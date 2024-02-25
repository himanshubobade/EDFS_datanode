import asyncio
import json
import os

from edfs.config import *
from edfs.edfs_datanode import EDFSDataNode

async def main():
    datanode = await EDFSDataNode.create_instance(LOCAL_HOST, DATANODE_C_PORT, "C")
    await datanode.register()
    await datanode.serve()

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())