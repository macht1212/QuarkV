# tests/test_RespParser.py
import asyncio
import contextlib
import os

# Импорт модуля — выберите вариант, который у вас собирается
# from kvxdb.core import respParser as rp
# или так, если вам удобнее путь через src:
from src.kvxdb.core import respParser as rp


async def read_one(reader: asyncio.StreamReader) -> bytes:
    """Прочитать ОДИН ответ RESP2 из asyncio-стрима."""
    line = await reader.readuntil(b"\r\n")
    t = line[:1]
    if t in (b"+", b"-", b":"):
        return line
    if t == b"$":  # bulk
        n = int(line[1:-2])
        if n == -1:
            return line
        data = await reader.readexactly(n + 2)  # payload + CRLF
        return line + data
    if t == b"*":  # array
        count = int(line[1:-2])
        if count == -1:
            return line
        parts = [line]
        for _ in range(count):
            parts.append(await read_one(reader))
        return b"".join(parts)
    raise RuntimeError(f"Unknown reply type byte: {t!r}")


async def _roundtrip():
    # базовые проверки кодера/парсера
    assert rp.encode_simple("OK") == b"+OK\r\n"
    p = rp.RespParser()
    assert len(p.feed(b"*1\r\n$4\r\nPING\r\n")) == 1

    uds = "/tmp/resp_py.sock"
    srv = rp.RespServer(uds)
    task = asyncio.create_task(srv.start())

    # дождёмся появления сокета (надёжнее, чем фиксированный sleep)
    for _ in range(200):
        if os.path.exists(uds):
            break
        await asyncio.sleep(0.01)

    reader, writer = await asyncio.open_unix_connection(uds)

    writer.write(rp.encode_command("PING"))
    await writer.drain()
    got = await read_one(reader)
    assert got == b"+PONG\r\n"

    writer.write(rp.encode_command("SET", "k", "41"))
    await writer.drain()
    got = await read_one(reader)
    assert got == b"+OK\r\n"

    writer.write(rp.encode_command("INCR", "k"))
    await writer.drain()
    got = await read_one(reader)
    assert got == b":42\r\n"

    writer.write(rp.encode_command("GET", "k"))
    await writer.drain()
    got = await read_one(reader)
    assert got == b"$2\r\n42\r\n"

    writer.close()
    await writer.wait_closed()

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def test_resp_server_roundtrip():
    # Запускаем асинхронную проверку в рамках теста
    asyncio.run(_roundtrip())
