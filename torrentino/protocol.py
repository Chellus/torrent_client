import asyncio
import logging
import struct
from asyncio import Queue
from concurrent.futures import CancelledError

import bitstring

# size of block to be requested is 16KB
REQUEST_SIZE = 2**14

class ProtocolError(BaseException):
    pass

class PeerConnection:
    """
    A peer connection used to download and upload pieces.

    The peer connection will consume one peer from the queue and
    try to perform a handshake.

    After a successful handshake, the connection will be in a choked and not interested state,
    not allowed to request pieces. After sending an interested message, the connection will wait
    to be unchoked. Once the remote peer unchoked us, we can start requesting pieces.

    The PeerConnection will keep requesting pieces for as long as there are pieces left to request,
    or until the remote peer disconnects.

    If the connection with a remote peer drops, the PeerConnection will consume the next available
    peer from off the queue and try to connect to that one.
    """
    def __init__(self, queue: Queue, info_hash, peer_id, piece_manager, on_block_cb=None):
        self.my_state = []
        self.peer_state = []
        self.queue = queue
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.remote_id = None
        self.writer = None
        self.reader = None
        self.piece_manager = piece_manager
        self.on_block_cb = on_block_cb
        self.future = asyncio.ensure_future(self._start())

    async def _start(self):
        while 'stopped' not in self.my_state:
            ip, port = await self.queue.get()
            logging.info(f'Got assigned peer with ip: {ip}')

            try:
                self.reader, self.writer = await asyncio.open_connection(ip, port)
                logging.info(f'Connection open to peer {ip}')

                # initiate handshake
                buffer = await self._handshake()

                self.my_state.append('choked')

                await self._send_interested()
                self.my_state.append('interested')

                async for message in PeerStreamIterator(self.reader, buffer):
                    if 'stopped' in self.my_state:
                        break
                    if type(message) is BitField:
                        self.piece_manager.add_peer(self.remote_id, message.bitfield)
                    elif type(message) is Interested:
                        self.peer_state.append('interested')
                    elif type(message) is NotInterested:
                        if 'interested' in self.peer_state:
                            self.peer_state.remove('interested')
                    elif type(message) is Choke:
                        self.my_state.append('choked')
                    elif type(message) is Unchoke:
                        if 'choked' in self.my_state:
                            self.my_state.remove('choked')
                    elif type(message) is Have:
                        self.piece_manager.update_peer(self.remote_id, message.index)
                    elif type(message) is KeepAlive:
                        pass
                    elif type(message) is Piece:
                        self.my_state.remove('pending_request')
                        self.on_block_cb(
                            peer_id=self.remote_id,
                            piece_index=message.index,
                            block_offset=message.begin,
                            data=message.block
                        )
                    elif type(message) is Request:
                        # TODO add support for sending data
                        logging.info('Ignoring the received request message.')
                    elif type(message) is Cancel:
                        # TODO add support for sending data
                        logging.info('Ignoring the received cancel message.')

                    if 'choked' not in self.my_state:
                        if 'interested' in self.my_state:
                            if 'pending_request' not in self.my_state:
                                self.my_state.append('pending_request')
                                await self._request_piece()

            except ProtocolError as e:
                logging.exception('Protocol error')
            except (ConnectionRefusedError, TimeoutError):
                logging.warning('Unable to connect to peer')
            except (ConnectionResetError, CancelledError):
                logging.warning('Connection closed')
            except Exception as e:
                logging.exception('An error occurred')
                self.cancel()
                raise e
            self.cancel()
    
    def cancel(self):
        """
        Sends the cancel message to the peer and closes the connection
        """
        logging.info(f'Closing peer {self.remote_id}')
        if not self.future.done():
            self.future.cancel()
        if self.writer:
            self.writer.close()
        
        self.queue.task_done()
    
    def stop(self):
        """
        Stop this connection from the current peer and from connecting to any new peer
        """
        self.my_state.append('stopped')
        if not self.future.done():
            self.future.cancel()

    async def _request_piece(self):
        block = self.piece_manager.next_request(self.remote_id)
        if block:
            message = Request(block.piece, block.offset, block.length).encode()

            logging.debug(f'Requesting block from {block.offset} for piece '
             f'{block.piece} of {block.length} bytes from peer {self.remote_id}')

    async def _handshake(self):
        """
        Send the initial handshake to remote peer and wait for the peer to respond
        with its handshake.
        """
        self.writer.write(Handshake(self.info_hash, self.peer_id).encode())
        await self.writer.drain()

        buf = b''
        tries = 1
        while len(buf) < Handshake.length and tries < 10:
            tries += 1
            buf = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
        
        response = Handshake.decode(buf[:Handshake.length])
        if not response:
            raise ProtocolError("Unable to receive and parse a handshake")
        if not response.info_hash == self.info_hash:
            raise ProtocolError("Handshake with invalid hash")

        # TODO according to spec we should validate that the peer_id received
        # from the peer matches the peer_id received by the tracker
        self.remote_id = response.peer_id
        logging.info('Handshake with peer was succesful')

        return buf[Handshake.length:]

    async def _send_interested(self):
        message = Interested()
        logging.debug(f'Sending message: {message}')
        self.writer.write(message.encode())
        await self.writer.drain()

    
class PeerStreamIterator:
    CHUNK_SIZE = 10*1024

    def __init__(self, reader, initial: bytes=None):
        self.reader = reader
        self.buffer = initial if initial else b''

    async def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                if data:
                    self.buffer += data
                    message = self.parse()
                    if message:
                        return message
                else:
                    logging.debug('No data from stream')
                    if self.buffer:
                        message = self.parse()
                        if message:
                            return message
                    raise StopAsyncIteration()
            except ConnectionResetError:
                logging.debug('Connection closed by a peer')
                raise StopAsyncIteration()
            except CancelledError:
                raise StopAsyncIteration()
            except StopAsyncIteration as e:
                raise e
            except Exception:
                logging.exception('Error when iterating over stream!')
                raise StopAsyncIteration()
        raise StopAsyncIteration()

    def parse(self):
        """
        Tries to parse protocol messages.

        :return The parsed message, or None if no message could be parsed.
        """
        # Each message is structured as <length prefix><message_id><payload>
        # length prefix: four byte big endian value
        # message id: single decimal byte
        # payload: message dependent
        
        header_length = 4

        if len(self.buffer) > 4:
            message_length = struct.unpack('>I', self.buffer[0:4])[0]

            if message_length == 0:
                return KeepAlive()
            
            if len(self.buffer) >= message_length:
                message_id = struct.unpack('>b', self.buffer[4:5])[0]

                def _consume():
                    self.buffer = self.buffer[header_length + message_length:]
                
                def _data():
                    return self.buffer[:header_length + message_length]
                
                if message_id is PeerMessage.BitField:
                    data = _data()
                    _consume()
                    return BitField.decode(data)
                elif message_id is PeerMessage.Interested:
                    _consume()
                    return Interested()
                elif message_id is PeerMessage.NotInterested:
                    _consume()
                    return NotInterested()
                elif message_id is PeerMessage.Choke:
                    _consume()
                    return Choke()
                elif message_id is PeerMessage.Unchoke:
                    _consume()
                    return Unchoke()
                elif message_id is PeerMessage.Have:
                    data = _data()
                    _consume()
                    return Have.decode(data)
                elif message_id is PeerMessage.Piece:
                    data = _data()
                    _consume()
                    return Piece.decode(data)
                elif message_id is PeerMessage.Request:
                    data = _data()
                    _consume()
                    return Request.decode(data)
                elif message_id is PeerMessage.Cancel:
                    data = _data()
                    _consume()
                    return Cancel.decode(data)
                else:
                    logging.info('Unsupported message!')
            else:
                logging.debug('Not enough in buffer in order to parse')
        return None
        
class PeerMessage:
    Choke = 0 #
    Unchoke = 1 #
    Interested = 2 #
    NotInterested = 3 #
    Have = 4 #
    BitField = 5 #
    Request = 6 #
    Piece = 7 #
    Cancel = 8 #
    Port = 9
    Handshake = None 
    KeepAlive = None #

    def encode(self) -> bytes:
        pass

    @classmethod
    def decode(self):
        pass

class Handshake(PeerMessage):
    length = 49 + 19

    def __init__(self, info_hash: bytes, peer_id: bytes):
        if isinstance(info_hash, str):
            info_hash = info_hash.encode('utf-8')
        if isinstance(peer_id, str):
            peer_id = peer_id.encode('utf-8')
        self.info_hash = info_hash
        self.peer_id = peer_id

    def encode(self) -> bytes:
        """
        Encodes this object instance to raw bytes representing the message
        """
        return struct.pack(">B19s8x20s20s", 19, b'BitTorrent protocol', self.info_hash, self.peer_id)

    @classmethod
    def decode(cls, data: bytes):
        """
        Decodes the given BitTorrent message into a handshake message, if not valid, None is returned
        """
        logging.debug(f'Decoding the handshake of length: {len(data)}')
        if len(data) < (49 + 19):
            return None
        parts = struct.unpack(">B19s8x20s20s", data)
        return cls(info_hash=parts[2], peer_id=parts[3])
    
    def __str__(self) -> str:
        return 'Handshake'

class KeepAlive(PeerMessage):
    def __str__(self) -> str:
        return 'KeepAlive'

class BitField(PeerMessage):
    def __init__(self, data):
        self.bitfield = bitstring.BitArray(bytes=data)

    def encode(self) -> bytes:
        bits_length = len(self.bitfield)
        return struct.pack(">Ib" + str(bits_length) + "s", 1 + bits_length, PeerMessage.BitField, self.bitfield)
    
    @classmethod
    def decode(cls, data: bytes):
        msg_length = struct.unpack(">I", data[:4])[0]
        logging.debug(f'Decoding BitField of length: {msg_length}')

        parts = struct.unpack(">Ib" + str(msg_length) + "s", data)
        return cls(parts[2])

    def __str__(self) -> str:
        return 'BitField'

class Interested(PeerMessage):
    def encode(self) -> bytes:
        return struct.pack(">Ib", 1, PeerMessage.Interested)
    
    def __str__(self) -> str:
        return 'Interested'

class NotInterested(PeerMessage):
    def encode(self) -> bytes:
        return struct.pack(">Ib", 1, PeerMessage.NotInterested)
    
    def __str__(self) -> str:
        return 'NotInterested'

class Choke(PeerMessage):
    def encode(self) -> bytes:
        return struct.pack(">Ib", 1, PeerMessage.Choke)
    
    def __str__(self) -> str:
        return 'Choke'

class Unchoke(PeerMessage):
    def encode(self) -> bytes:
        return struct.pack(">Ib", 1, PeerMessage.Unchoke)
    
    def __str__(self) -> str:
        return 'Unchoke'

class Have(PeerMessage):
    """
    The Have message format is: <len=0005><id=4><piece index>
    The payload is the index of a piece that has just been successfully downloaded
    and verified via the hash.
    """

    def __init__(self, index: int):
        self.index = index
    
    def encode(self):
        return struct.pack(">IbI", 5, PeerMessage.Have, self.index)

    @classmethod
    def decode(cls, data: bytes):
        index = struct.unpack(">IbI", data)[2]
        return cls(index)
    
    def __str__(self) -> str:
        return 'Have'

class Request(PeerMessage):
    """
    The request message is fixed length, and is used to request a block.
    request: <len=0013><id=6><index><begin><length>
    index: integer specifying the zero-based piece index
    begin: integer specyfing the zero-based byte offset within the piece
    length: int specifying the requested length
    """

    def __init__(self, index: int, begin: int, length: int = REQUEST_SIZE):
        self.index = index
        self.begin = begin
        self.length = length
    
    def encode(self):
        return struct.pack(">IbIII", 13, PeerMessage.Request, self.index, self.begin, self.length)

    @classmethod
    def decode(cls, data: bytes):
        # tuple with (msg length, id, index, begin, length)
        parts = struct.unpack(">IbIII", data)
        return cls(parts[2], parts[3], parts[4])
    
    def __str__(self) -> str:
        return 'Request'

class Piece(PeerMessage):
    """
    The piece message is variable length, where X is the length of the block.
    piece: <len=0009+X><id=7><index><begin><block>
    index: integer specifying the zero-based piece index
    begin: integer specifying the zero-based byte offset within the piece
    block: block of data, which is a subset of the piece specified by index.
    """
    length = 9

    def __init__(self, index: int, begin: int, block: bytes):
        self.index = index
        self.begin = begin
        self.block = block
    
    def encode(self):
        msg_length = Piece.length + len(self.block)
        return struct.pack(">IbII" + str(len(self.block)) + 's',
        msg_length, PeerMessage.Piece, self.index, self.begin, self.block)

    @classmethod
    def decode(cls, data: bytes):
        length = struct.unpack(">I", data[:4])[0]
        parts = struct.unpack(">IbII" + str(length - Piece.length) + 's', data[:length+4])
        return cls(parts[2], parts[3], parts[4])

    def __str__(self) -> str:
        return 'Piece'

class Cancel(PeerMessage):
    """
    The cancel message is fixed length, and is used to cancel block requests.
    The payload is identical to that of the "request" message.
    cancel: <len=0013><id=8><index><begin><length>
    """

    def __init__(self, index: int, begin: int, length: int=REQUEST_SIZE):
        self.index = index
        self.begin = begin
        self.length = length
    
    def encode(self):
        return struct.pack(">IbIII", 13, PeerMessage.Cancel, self.index, self.begin, self.length)

    @classmethod
    def decode(cls, data: bytes):
        parts = struct.unpack(">IbIII", data)
        return cls(parts[2], parts[3], parts[4])

    def __str__(self) -> str:
        return 'Cancel'    
        