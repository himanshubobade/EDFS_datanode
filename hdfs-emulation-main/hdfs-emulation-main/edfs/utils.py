class PacketUtils:
    @staticmethod
    def get_chunk_header(bytes_len):
        header = []
        for i in range(4):
            header.append((bytes_len >> (i * 8)) & 0xff)
        return bytearray(header[::-1])

    @staticmethod
    def chunk_header_to_len(header):
        data_len = 0
        for i in range(4):
            data_len = data_len * 256 + header[i]
        return data_len;

    @staticmethod
    def encode(data):
        return PacketUtils.get_chunk_header(len(data)) + data

    @staticmethod
    def decode(buf):
        data_len = PacketUtils.chunk_header_to_len(buf[:4])
        return buf[4: 4 + data_len]

    # return a list of packets and the current ptr of the buffer
    @staticmethod
    def create_packets_from_buffer(buf):
        packets = []
        ptr = 0
        while ptr < len(buf):
            if len(buf) - ptr < 4: break
            packet_data_len = PacketUtils.chunk_header_to_len(buf[ptr: ptr + 4])
            if ptr + 4 + packet_data_len > len(buf): break
            packet_data = buf[ptr + 4: ptr + 4 + packet_data_len]
            packets.append(packet_data)
            ptr += (4 + packet_data_len)
        return packets, ptr
