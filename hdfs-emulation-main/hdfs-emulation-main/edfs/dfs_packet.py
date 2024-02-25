from edfs.config import *

class DFSPacket:
    seqno = 0

    @classmethod
    def create_packet(cls, data, last_packet_in_block):
        self = DFSPacket(data, last_packet_in_block)
        DFSPacket.seqno += 1
        self.seqno = DFSPacket.seqno
        return self

    def __init__(self, data, last_packet_in_block):
        self.data = data
        self.num_byte = len(data)
        self.last_packet_in_block = last_packet_in_block

    def get_seqno(self):
        return self.seqno

    def get_data(self):
        return self.data

    def get_datalen(self):
        return len(self.data)

    def is_last_packet_in_block(self):
        return self.last_packet_in_block

    def set_last_packet_in_block(self, last_packet_in_block):
        self.last_packet_in_block
