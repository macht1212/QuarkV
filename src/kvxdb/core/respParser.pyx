# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True
# distutils: language = c++

from __future__ import annotations
import asyncio
import os

from typing import Optional, List, Tuple, Union

import cython

CRLF = b"\r\n"

# ---------------- Exceptions ----------------
class RespError(Exception):
    pass


# ---------------- Value wrappers ----------------
cdef class SimpleString:
    cdef public str value
    def __init__(self, value: str): self.value = value

cdef class ErrorString:
    cdef public str value
    def __init__(self, value: str): self.value = value

cdef class Integer:
    cdef public long long value
    def __init__(self, value: int): self.value = int(value)

cdef class BulkString:
    cdef public object value  # bytes | None
    def __init__(self, value: Optional[bytes]): self.value = value

cdef class Array:
    cdef public object value  # list[Any] | None
    def __init__(self, value: Optional[List]): self.value = value


# ---------------- Encoding  ----------------
@cython.cfunc
@cython.inline
def _encode_int_c(long long n) -> bytes:
    return str(n).encode("ascii")

@cython.cfunc
@cython.inline
def _ensure_ascii_c(s: str) -> bytes:
    if '\r' in s or '\n' in s:
        raise RespError("Simple/Error string cannot contain CR or LF")
    return s.encode("utf-8")

cpdef bytes encode_simple(str s):
    return b"+" + _ensure_ascii_c(s) + CRLF

cpdef bytes encode_error(str s):
    return b"-" + _ensure_ascii_c(s) + CRLF

cpdef bytes encode_integer(long long n):
    return b":" + _encode_int_c(n) + CRLF

cpdef bytes encode_bulk(object data):
    """
    data: Optional[Union[bytes, str]]
    """
    if data is None:
        return b"$-1" + CRLF
    cdef bytes b
    if isinstance(data, str):
        b = (<str>data).encode("utf-8")
    elif isinstance(data, (bytes, bytearray, memoryview)):
        b = bytes(data)
    else:
        raise TypeError("bulk expects bytes|str|None")
    return b"$" + _encode_int_c(<long long>len(b)) + CRLF + b + CRLF

cpdef bytes encode_array(object items):
    """
    items: Optional[List[Any]]
    """
    if items is None:
        return b"*-1" + CRLF
    cdef list lst = <list>items
    cdef list chunks = [b"*", _encode_int_c(<long long>len(lst)), CRLF]
    cdef object it
    for it in lst:
        chunks.append(encode(it))
    return b"".join(chunks)

cpdef bytes encode(object value):
    if isinstance(value, SimpleString):
        return encode_simple((<SimpleString>value).value)
    if isinstance(value, ErrorString):
        return encode_error((<ErrorString>value).value)
    if isinstance(value, Integer):
        return encode_integer((<Integer>value).value)
    if isinstance(value, BulkString):
        return encode_bulk((<BulkString>value).value)
    if isinstance(value, Array):
        return encode_array((<Array>value).value)
    if isinstance(value, int):
        return encode_integer(value)
    if isinstance(value, (bytes, str)) or value is None:
        return encode_bulk(value)
    if isinstance(value, list):
        return encode_array(value)
    raise RespError(f"Cannot encode type {type(value)}")

def encode_command(*parts: Union[str, bytes]):
    cdef list items = []
    cdef object p
    for p in parts:
        if isinstance(p, str):
            items.append(BulkString((<str>p).encode("utf-8")))
        elif isinstance(p, (bytes, bytearray, memoryview)):
            items.append(BulkString(bytes(p)))
        else:
            raise TypeError("command parts must be str or bytes")
    return encode(Array(items))


# ---------------- Incremental Parser ----------------
cdef class RespParser:
    cdef public bytearray buf
    cdef public Py_ssize_t pos

    def __cinit__(self):
        self.buf = bytearray()
        self.pos = 0

    cpdef reset(self):
        self.buf.clear()
        self.pos = 0

    cpdef list feed(self, bytes data):
        self.buf.extend(data)
        cdef list values = []
        cdef object res
        cdef Py_ssize_t new_pos
        while True:
            res = self._parse_at(self.pos)
            if res is None:
                break
            # res -> (val, new_pos)
            values.append(res[0])
            new_pos = <Py_ssize_t>res[1]
            self.pos = new_pos
            if self.pos > 0 and self.pos >= 4096 and self.pos > len(self.buf) // 2:
                del self.buf[:self.pos]
                self.pos = 0

        if self.pos > 0 and self.pos == len(self.buf):
            self.buf.clear()
            self.pos = 0
        elif self.pos > 0 and self.pos > len(self.buf) // 2:
            del self.buf[:self.pos]
            self.pos = 0
        return values

    cdef object _readline(self, Py_ssize_t pos):
        cdef Py_ssize_t idx = self._find_crlf(pos)
        if idx == -1:
            return None
        # bytes(self.buf[pos:idx]) создаёт копию — как и в оригинале
        line = bytes(self.buf[pos:idx])
        return (line, idx + 2)

    @cython.cfunc
    @cython.inline
    def _find_crlf(self, Py_ssize_t pos):
        cdef Py_ssize_t n = len(self.buf)
        cdef unsigned char cr = 13  # '\r'
        cdef unsigned char lf = 10  # '\n'
        cdef unsigned char c
        cdef Py_ssize_t i = pos
        # memoryview по bytearray
        cdef unsigned char[:] mv = self.buf
        while i + 1 < n:
            c = mv[i]
            if c == cr and mv[i + 1] == lf:
                return i
            i += 1
        return -1

    cdef object _parse_at(self, Py_ssize_t pos):
        if pos >= len(self.buf):
            return None
        cdef unsigned char[:] mv = self.buf
        cdef int b = mv[pos]
        cdef object line
        cdef Py_ssize_t after, end
        cdef bytes data
        cdef long long n
        cdef long long num
        cdef list items
        cdef Py_ssize_t cur
        cdef int i

        if b == ord(b'+'):
            line = self._readline(pos + 1)
            if line is None:
                return None
            data = <bytes>line[0]
            after = <Py_ssize_t>line[1]
            return (SimpleString(data.decode("utf-8")), after)

        elif b == ord(b'-'):
            line = self._readline(pos + 1)
            if line is None:
                return None
            data = <bytes>line[0]
            after = <Py_ssize_t>line[1]
            try:
                s = data.decode("utf-8")
            except UnicodeDecodeError:
                s = data.decode("latin-1")
            return (ErrorString(s), after)

        elif b == ord(b':'):
            line = self._readline(pos + 1)
            if line is None:
                return None
            data = <bytes>line[0]
            after = <Py_ssize_t>line[1]
            try:
                num = int(data.decode("ascii"))
            except Exception:
                raise RespError("Invalid integer")
            return (Integer(num), after)

        elif b == ord(b'$'):
            line = self._readline(pos + 1)
            if line is None:
                return None
            data = <bytes>line[0]
            after = <Py_ssize_t>line[1]
            try:
                n = int(data.decode("ascii"))
            except Exception:
                raise RespError("Invalid bulk length")
            if n == -1:
                return (BulkString(None), after)
            end = after + n + 2
            if end > len(self.buf):
                return None
            # bytes(...) — копия полезной нагрузки
            data = bytes(self.buf[after:after + n])
            if self.buf[after + n: end] != CRLF:
                raise RespError("Bulk not terminated with CRLF")
            return (BulkString(data), end)

        elif b == ord(b'*'):
            line = self._readline(pos + 1)
            if line is None:
                return None
            data = <bytes>line[0]
            after = <Py_ssize_t>line[1]
            try:
                n = int(data.decode("ascii"))
            except Exception:
                raise RespError("Invalid array length")
            if n == -1:
                return (Array(None), after)
            items = []
            cur = after
            for i in range(<int>n):
                line = self._parse_at(cur)
                if line is None:
                    return None
                items.append(line[0])
                cur = <Py_ssize_t>line[1]
            return (Array(items), cur)

        else:
            raise RespError(f"Unknown type byte: {bytes([b])!r}")


# ---------------- Asyncio Unix-socket demo server ----------------
cdef class RespServer:
    cdef public str uds_path
    cdef public dict store

    def __init__(self, uds_path: str = "/tmp/resp_py.sock"):
        self.uds_path = uds_path
        self.store = {}

    async def start(self):
        try:
            os.unlink(self.uds_path)
        except FileNotFoundError:
            pass
        server = await asyncio.start_unix_server(self.handle_client, path=self.uds_path)
        print(f"RESP2 demo server listening on {self.uds_path}")
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        parser = RespParser()
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                try:
                    messages = parser.feed(data)
                except RespError as e:
                    writer.write(encode_error(f"ERR {e}"))
                    await writer.drain()
                    break
                out_chunks = []
                for msg in messages:
                    out_chunks.append(self.dispatch(msg))
                if out_chunks:
                    writer.write(b"".join(out_chunks))
                    await writer.drain()
        except ConnectionResetError:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    cdef bytes _as_bulk_or_error(self, object maybe_bulk):
        cdef object val
        if not isinstance(maybe_bulk, BulkString) or (<BulkString>maybe_bulk).value is None:
            raise RespError("expected bulk string")
        val = (<BulkString>maybe_bulk).value
        return <bytes>val

    def dispatch(self, msg) -> bytes:
        # Все Cython-переменные — в начале
        cdef list elems
        cdef object v
        cdef long long cur

        if not isinstance(msg, Array) or (<Array>msg).value is None or len((<Array>msg).value) == 0:
            return encode_error("ERR Protocol: expected array")
        elems = (<Array>msg).value

        def as_text(bulk: BulkString) -> str:
            if not isinstance(bulk, BulkString) or bulk.value is None:
                raise RespError("expected bulk string")
            try:
                return (<bytes>bulk.value).decode("utf-8")
            except UnicodeDecodeError:
                return (<bytes>bulk.value).decode("latin-1")

        try:
            cmd = as_text(<BulkString>elems[0]).upper()
        except RespError as e:
            return encode_error(f"ERR {e}")
        try:
            if cmd == "PING":
                if len(elems) == 1:
                    return encode_simple("PONG")
                elif len(elems) == 2 and isinstance(elems[1], BulkString):
                    return encode_bulk((<BulkString>elems[1]).value)
                else:
                    return encode_error("ERR wrong number of arguments for 'PING'")
            elif cmd == "ECHO":
                if len(elems) != 2 or not isinstance(elems[1], BulkString):
                    return encode_error("ERR wrong number of arguments for 'ECHO'")
                return encode_bulk((<BulkString>elems[1]).value)
            elif cmd == "SET":
                if len(elems) != 3:
                    return encode_error("ERR wrong number of arguments for 'SET'")
                key = as_text(<BulkString>elems[1])
                v = (<BulkString>elems[2]).value if isinstance(elems[2], BulkString) else None
                if v is None:
                    return encode_error("ERR SET requires bulk value")
                self.store[key] = bytes(<bytes>v)
                return encode_simple("OK")
            elif cmd == "GET":
                if len(elems) != 2:
                    return encode_error("ERR wrong number of arguments for 'GET'")
                key = as_text(<BulkString>elems[1])
                if key in self.store:
                    return encode_bulk(self.store[key])
                else:
                    return encode_bulk(None)
            elif cmd == "INCR":
                if len(elems) != 2:
                    return encode_error("ERR wrong number of arguments for 'INCR'")
                key = as_text(<BulkString>elems[1])
                cur = int(self.store.get(key, b"0"))
                cur += 1
                self.store[key] = str(cur).encode("ascii")
                return encode_integer(cur)
            else:
                return encode_error("ERR unknown command")
        except Exception as e:
            return encode_error(f"ERR {e}")
