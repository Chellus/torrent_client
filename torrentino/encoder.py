# BENCODING ENCODER

from collections import OrderedDict

class Encoder:
    def __init__(self, data):
        self._data = data
    
    def encode(self) -> bytes:
        return self.encode_next(self._data)

    def encode_next(self, data):
        if type(data) == str:
            return self.encode_str(data)
        elif type(data) == int:
            return self.encode_int(data)
        elif type(data) == list:
            return self.encode_list(data)
        elif type(data) == dict or type(data) == OrderedDict:
            return self.encode_dict(data)
        elif type(data) == bytes:
            return self.encode_bytes(data)
        else:
            return None

    def encode_int(self, value):
        return str.encode('i' + str(value) + 'e')
    
    def encode_str(self, value: str):
        res = str(len(value)) + ':' + value
        return str.encode(res)

    def encode_bytes(self, value: str):
        result = bytearray()
        result += str.encode(str(len(value)))
        result += b':'
        result += value
        return result

    def encode_list(self, data):
        result = bytearray('l', 'utf-8')
        result += b''.join([self.encode_next(item)] for item in data)
        result += b'e'
        return result

    def encode_dict(self, data: dict) -> bytes:
        result = bytearray('d', 'utf-8')
        for k, v in data.items():
            key = self.encode_next(k)
            value = self.encode_next(v)
            if key and value:
                result += key
                result += value
            else:
                raise RuntimeError('Bad dict')
        
        result += b'e'
        return result