# tests/test_reactor.py
import os
import socket
import threading
import time
import sys

import pytest

try:
    from kvxdb.core.reactor import Reactor
except Exception as e:
    pytest.skip(f"Reactor import failed: {e}", allow_module_level=True)

def _pick_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port

def _run_reactor_in_thread(port, **kwargs):
    done = threading.Event()
    err = {"exc": None}
    # дефолтный PING/PONG парсер, если не передан свой
    def default_on_request(mv):
        data = bytes(mv)
        if b"\n" in data:
            line, _ = data.split(b"\n", 1)
            if line == b"PING":
                return len(line)+1, b"PONG\n", False
            return len(line)+1, b"", False
        return 0, None, False
    on_request_cb = kwargs.pop("on_request_cb", default_on_request)

    r = Reactor(port=port, on_request_cb=on_request_cb, **kwargs)
    th = threading.Thread(target=lambda: _reactor_run(r, done, err), daemon=True)
    th.start()
    # небольшая пауза, чтобы сокет начал слушать
    time.sleep(0.05)
    return r, th, done, err

def _reactor_run(r, done_evt, err_box):
    try:
        r.run()
    except Exception as e:
        err_box["exc"] = e
    finally:
        done_evt.set()

def _stop_reactor(r, th):
    r.stop()
    th.join(timeout=2.0)
    assert not th.is_alive(), "reactor thread did not stop in time"

def _conn(port, timeout=1.0):
    return socket.create_connection(("127.0.0.1", port), timeout=timeout)

def test_ping_pong():
    port = _pick_free_port()
    r, th, done, err = _run_reactor_in_thread(port, tick_ms=20, max_conns=256, max_bulk=4*1024*1024)
    try:
        s = _conn(port)
        s.sendall(b"PING\n")
        data = s.recv(64)
        s.close()
        assert data == b"PONG\n"
        assert err["exc"] is None
    finally:
        _stop_reactor(r, th)

def test_pipeline_three():
    port = _pick_free_port()
    r, th, done, err = _run_reactor_in_thread(port, tick_ms=20, max_conns=256, max_bulk=4*1024*1024)
    try:
        s = _conn(port)
        s.sendall(b"PING\nPING\nPING\n")
        time.sleep(0.05)
        data = s.recv(1024)
        s.close()
        assert data.count(b"PONG\n") == 3
    finally:
        _stop_reactor(r, th)

def test_tick_called():
    port = _pick_free_port()
    ticks = {"n": 0}
    def on_tick():
        ticks["n"] += 1
    r, th, done, err = _run_reactor_in_thread(port, tick_ms=20, on_tick_cb=on_tick)
    try:
        time.sleep(0.12)
        assert ticks["n"] >= 3  # примерно раз в 20мс
    finally:
        _stop_reactor(r, th)

def test_max_bulk_close():
    port = _pick_free_port()
    # max_bulk = 1024, пошлём 2048 байт, сервер должен закрыть соединение
    r, th, done, err = _run_reactor_in_thread(port, max_bulk=1024)
    try:
        s = _conn(port)
        s.sendall(b"A" * 2048)
        time.sleep(0.05)
        try:
            data = s.recv(16)
        except ConnectionResetError:
            data = b""
        s.close()
        assert data == b"", "expected server to close connection due to max_bulk"
    finally:
        _stop_reactor(r, th)

def test_max_conns_limit():
    port = _pick_free_port()
    maxc = 8
    r, th, done, err = _run_reactor_in_thread(port, max_conns=maxc)
    conns = []
    try:
        # откроем ровно maxc соединений
        for _ in range(maxc):
            conns.append(_conn(port))
        # а вот это лишнее — сервер должен закрыть его сразу
        extra = socket.socket()
        extra.settimeout(1.0)
        extra.connect(("127.0.0.1", port))
        # попробуем отправить и прочитать — ожидаем закрытие
        try:
            extra.sendall(b"PING\n")
            time.sleep(0.02)
            d = extra.recv(16)
        except (ConnectionResetError, BrokenPipeError, TimeoutError, socket.timeout):
            d = b""
        finally:
            extra.close()
        assert d == b"", "extra connection should be refused/closed"
    finally:
        for s in conns:
            try:
                s.close()
            except Exception:
                pass
        _stop_reactor(r, th)

@pytest.mark.timeout(5)
def test_large_write_partial_path():
    """
    Форсим путь частичных write: колбэк возвращает большой ответ (~1MB).
    Проверяем, что клиент получает полностью и сервер не падает.
    """
    port = _pick_free_port()
    big = b"X" * (1 << 20)  # 1 MiB
    def on_request(mv):
        data = bytes(mv)
        if b"\n" in data:
            line, _ = data.split(b"\n", 1)
            if line == b"BIG":
                return len(line)+1, big, False
            return len(line)+1, b"", False
        return 0, None, False

    r, th, done, err = _run_reactor_in_thread(port, on_request_cb=on_request)
    try:
        s = _conn(port, timeout=3.0)
        s.sendall(b"BIG\n")
        got = b""
        while len(got) < len(big):
            chunk = s.recv(65536)
            if not chunk:
                break
            got += chunk
        s.close()
        assert got == big
        assert err["exc"] is None
    finally:
        _stop_reactor(r, th)
