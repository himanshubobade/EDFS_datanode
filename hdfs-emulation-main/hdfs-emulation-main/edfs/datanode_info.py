class DataNodeInfo:
    LAST_DATANODE_ID = 1
    def __init__(self, ip, port, name):
        self.id = DataNodeInfo.LAST_DATANODE_ID
        self.ip = ip
        self.port = port
        self.name = name
        DataNodeInfo.LAST_DATANODE_ID += 1

    def get_id(self):
        return self.id

    def get_ip(self):
        return self.ip

    def get_port(self):
        return self.port

    def get_name(self):
        return self.name

    def get_info(self):
        return {"ip": self.ip, "port": self.port, "name": self.name}

class DataNodeManager:
    def __init__(self):
        self.id_to_datanode = {}

    def get_all_datanodes(self):
        return list(self.id_to_datanode.values())

    def get_datanode_by_id(self, id):
        return self.id_to_datanode.get(id)

    def register(self, datanode_info):
        self.id_to_datanode[datanode_info.get_id()] = datanode_info
