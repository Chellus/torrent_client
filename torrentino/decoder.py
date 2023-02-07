# BENCODING DECODER

from collections import OrderedDict

# indicates start of integers
TOKEN_INTEGER = b'i'

# indicates start of list
TOKEN_LIST = b'l'

# indicates start of dict
TOKEN_DICT = b'd'

# indicates end of int, list and dict values
TOKEN_END = b'e'

# delimits string length from string data
TOKEN_STRING_SEPARATOR = b':'

class Decoder:
    def __init__(self, data: bytes):
        if not isinstance(data, bytes):
            raise TypeError('Argument "data" must be of type bytes')
        self._data = data
        self._index = 0
    
    def decode(self):
        """
        Checks data type to decode and decodes accordingly
        """
        c = self._peek()
        if c is None:
            raise EOFError("Unexpected EOF")

        elif c == TOKEN_INTEGER:
            self._index += 1
            return self.decode_int()
        
        elif c == TOKEN_LIST:
            self._index += 1
            return self.decode_list()

        elif c == TOKEN_DICT:
            self._index += 1
            return self.decode_dict()
        
        elif c in b'0123456789':
            return self.decode_string()
        
        else:
            raise RuntimeError(f"Invalid token read at {str(self._index)}")
    
    def _peek(self):
        """
        return the next character of the bencoded data
        returns None if there is no next character
        """
        
        if self._index + 1 >= len(self._data):
            return None
        return self._data[self._index:self._index + 1]
    
    def read(self, length : int) -> bytes:
        if self._index + length > len(self._data):
            raise IndexError(f'Cannot read {str(length)} bytes from current position {str(self._index)}')
        res = self._data[self._index:self._index + length]
        self._index += length
        return res

    def read_until(self, token) -> bytes:
        try:
            # look for first occurrence of provided token, starting from current index
            occurrence = self._data.index(token, self._index)
            # the result will be the data from previous index up to the token provided
            result = self._data[self._index:occurrence]
            # the new current index will be after the token at which we stopped reading
            self._index = occurrence + 1
            return result
        except ValueError:
            raise RuntimeError(f"Unable to find token {str(token)}")

    def decode_int(self):
        return int(self.read_until(TOKEN_END))

    def decode_list(self):
        res = []
        # recursive decode the content of the list
        while self._data[self._index: self._index + 1] != TOKEN_END:
            res.append(self.decode())
        self._index += 1
        return res

    def decode_dict(self):
        res = OrderedDict()
        while self._data[self._index: self._index + 1] != TOKEN_END:
            key = self.decode()
            obj = self.decode()
            res[key] = obj
        self._index += 1
        return res

    def decode_string(self):
        bytes_to_read = int(self.read_until(TOKEN_STRING_SEPARATOR))
        data = self.read(bytes_to_read)
        return data