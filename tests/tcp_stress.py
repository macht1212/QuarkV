# tests/tcp_stress.py
import socket, threading, time, sys

HOST, PORT = "127.0.0.1", int(sys.argv[1]) if len(sys.argv)>1 else 11211
N = int(sys.argv[2]) if len(sys.argv)>2 else 500

ok = 0
lock = threading.Lock()

def worker():
    global ok
    try:
        s = socket.create_connection((HOST, PORT), timeout=1)
        s.sendall(b"PING\n")
        d = s.recv(16)
        s.close()
        if d == b"PONG\n":
            with lock:
                ok += 1
    except Exception:
        pass

ts = [threading.Thread(target=worker) for _ in range(N)]
[t.start() for t in ts]
[t.join() for t in ts]
print(f"OK {ok}/{N}")
