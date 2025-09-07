# distutils: language = c
# cython: boundscheck=False, wraparound=False, cdivision=True, nonecheck=False, infer_types=True

from libc.time    cimport time
from cpython.memoryview cimport PyMemoryView_FromMemory
from cpython.buffer     cimport PyBUF_READ

from libc.stdint  cimport uint64_t
from libc.stddef  cimport size_t
ctypedef long ssize_t   # универсально для Linux/macOS

# ---------------- errno ----------------
cdef extern from "<errno.h>":
    int errno
    int EINTR
    int EAGAIN
    int EWOULDBLOCK

# --------------- fcntl.h ---------------
cdef extern from "<fcntl.h>":
    int fcntl(int fd, int cmd, ...) nogil
    int F_GETFL
    int F_SETFL
    int F_GETFD
    int F_SETFD
    int O_NONBLOCK
    int FD_CLOEXEC

# ------------- arpa/inet.h -------------
cdef extern from "<arpa/inet.h>":
    unsigned short htons(unsigned short hostshort)
    unsigned int   inet_addr(const char* cp)

# ------------- netinet/in.h ------------
cdef extern from "<netinet/in.h>":
    ctypedef unsigned short in_port_t
    ctypedef unsigned int   in_addr_t
    cdef struct in_addr:
        in_addr_t s_addr
    cdef struct sockaddr_in:
        unsigned short  sin_family
        in_port_t       sin_port
        in_addr         sin_addr
        char            sin_zero[8]

# ------------- sys/socket.h ------------
cdef extern from "<sys/socket.h>":
    cdef struct sockaddr:
        pass
    ctypedef unsigned int socklen_t

    int socket(int domain, int type, int protocol)
    int bind(int sockfd, const sockaddr *addr, socklen_t addrlen)
    int listen(int sockfd, int backlog)
    int setsockopt(int sockfd, int level, int optname, const void *optval, socklen_t optlen)
    int accept(int sockfd, void *addr, socklen_t *addrlen) nogil
    int shutdown(int sockfd, int how)

    cdef int AF_INET
    cdef int SOCK_STREAM
    cdef int SOL_SOCKET
    cdef int SO_REUSEADDR
    cdef int SO_REUSEPORT

# ---------------- unistд.h -------------
cdef extern from "<unistd.h>":
    int     c_close "close"(int fd) nogil
    ssize_t c_read  "read" (int fd, void* buf,  size_t count) nogil
    ssize_t c_write "write"(int fd, const void* buf, size_t count) nogil

# -------------- stdlib.h (nogil) -------
cdef extern from "<stdlib.h>":
    void* c_malloc  "malloc"(size_t) nogil
    void  c_free    "free"(void*) nogil
    void* c_realloc "realloc"(void*, size_t) nogil

# -------------- string.h (nogil) -------
cdef extern from "<string.h>":
    void* c_memcpy  "memcpy"(void*, const void*, size_t) nogil
    void* c_memmove "memmove"(void*, const void*, size_t) nogil
    void* c_memset  "memset"(void*, int, size_t) nogil
    int   c_memcmp  "memcmp"(const void*, const void*, size_t) nogil

# ---------------- poll.h ---------------
cdef extern from "<poll.h>":
    cdef struct pollfd:
        int   fd
        short events
        short revents
    int poll(pollfd* fds, int nfds, int timeout) nogil
    cdef int POLLIN
    cdef int POLLOUT
    cdef int POLLERR
    cdef int POLLHUP

# -------------- helpers ----------------
cdef inline uint64_t now_ms() noexcept nogil:
    # грубая миллисекундная метка — для тикера достаточно
    return <uint64_t>time(NULL) * 1000

cdef enum ConnState:
    CONN_RECV   = 0
    CONN_CLOSED = 2

cdef struct conn_t:
    int        fd
    ConnState  state
    char*      rbuf
    size_t     rcap
    size_t     rlen
    char*      wbuf
    size_t     wcap
    size_t     wlen
    size_t     woff
    uint64_t   last_active_ms
    conn_t*    next
    conn_t*    prev

cdef class Reactor:
    cdef:
        int lfd
        conn_t* head
        int     cur_conns
        int     max_conns
        size_t  max_bulk
        int     tick_ms
        object  on_request_cb
        object  on_tick_cb
        int     backlog
        int     running
        pollfd*  pfds
        conn_t** pconns
        int      pfds_cap

    def __cinit__(self, int port,
                  int tick_ms=20,
                  int max_conns=4096,
                  size_t max_bulk=32*1024*1024,
                  object on_request_cb=None,
                  object on_tick_cb=None,
                  int backlog=1024,
                  bytes host=b"0.0.0.0"):
        self.lfd = -1
        self.head = NULL
        self.cur_conns = 0
        self.max_conns = max_conns
        self.max_bulk = max_bulk
        self.tick_ms = tick_ms if tick_ms >= 10 else 10
        self.on_request_cb = on_request_cb
        self.on_tick_cb = on_tick_cb
        self.backlog = backlog
        self.running = 0
        self.pfds = NULL
        self.pconns = NULL
        self.pfds_cap = 0

        cdef int yes = 1
        cdef sockaddr_in addr

        # socket()
        self.lfd = socket(AF_INET, SOCK_STREAM, 0)
        if self.lfd < 0:
            raise OSError(errno, "socket failed")

        # CLOEXEC + NONBLOCK
        cdef int flags = fcntl(self.lfd, F_GETFD)
        fcntl(self.lfd, F_SETFD, flags | FD_CLOEXEC)
        flags = fcntl(self.lfd, F_GETFL)
        fcntl(self.lfd, F_SETFL, flags | O_NONBLOCK)

        # REUSEADDR / REUSEPORT
        if setsockopt(self.lfd, SOL_SOCKET, SO_REUSEADDR, &yes, <socklen_t>sizeof(int)) != 0:
            self._cleanup()
            raise OSError(errno, "setsockopt SO_REUSEADDR failed")
        setsockopt(self.lfd, SOL_SOCKET, SO_REUSEPORT, &yes, <socklen_t>sizeof(int))

        # bind()
        c_memset(&addr, 0, sizeof(addr))
        addr.sin_family = AF_INET
        addr.sin_port = htons(<unsigned short>port)
        cdef const char* host_c = host
        addr.sin_addr.s_addr = inet_addr(host_c)
        if bind(self.lfd, <const sockaddr*>&addr, <socklen_t>sizeof(addr)) != 0:
            self._cleanup()
            raise OSError(errno, "bind failed")

        if listen(self.lfd, self.backlog) != 0:
            self._cleanup()
            raise OSError(errno, "listen failed")

    cdef void _cleanup(self) noexcept nogil:
        if self.lfd >= 0:
            c_close(self.lfd)
        self.lfd = -1

    def close(self):
        self.stop()
        if self.lfd >= 0:
            c_close(self.lfd)
            self.lfd = -1

    def stop(self):
        self.running = 0

    # --- conn list management ---
    cdef inline void _list_add(self, conn_t* c) noexcept nogil:
        c.prev = NULL
        c.next = self.head
        if self.head != NULL:
            self.head.prev = c
        self.head = c

    cdef inline void _list_del(self, conn_t* c) noexcept nogil:
        if c.prev != NULL:
            c.prev.next = c.next
        else:
            self.head = c.next
        if c.next != NULL:
            c.next.prev = c.prev
        c.prev = c.next = NULL

    cdef inline conn_t* _mk_conn(self, int fd) noexcept nogil:
        cdef conn_t* c = <conn_t*>c_malloc(sizeof(conn_t))
        if c == NULL:
            return NULL
        c.fd = fd
        c.state = CONN_RECV
        c.rcap = 4096
        c.wcap = 4096
        c.rlen = 0
        c.wlen = 0
        c.woff = 0
        c.last_active_ms = now_ms()
        c.rbuf = <char*>c_malloc(c.rcap)
        c.wbuf = <char*>c_malloc(c.wcap)
        c.prev = NULL
        c.next = NULL
        if (c.rbuf == NULL) or (c.wbuf == NULL):
            if c.rbuf: c_free(c.rbuf)
            if c.wbuf: c_free(c.wbuf)
            c_free(c)
            return NULL
        return c

    cdef inline void _free_conn(self, conn_t* c) noexcept nogil:
        if c == NULL:
            return
        if c.fd >= 0:
            c_close(c.fd)
            c.fd = -1
        if c.rbuf != NULL:
            c_free(c.rbuf); c.rbuf = NULL
        if c.wbuf != NULL:
            c_free(c.wbuf); c.wbuf = NULL
        c_free(c)

    cdef inline int _ensure_cap(self, char** buf, size_t* cap, size_t need, size_t hard_max) noexcept nogil:
        if cap[0] >= need:
            return 0
        cdef size_t newcap = cap[0]
        while newcap < need:
            newcap = newcap * 2
            if newcap > hard_max:
                newcap = hard_max
                break
        if newcap < need:
            return -1
        cdef void* p = c_realloc(<void*>buf[0], newcap)
        if p == NULL:
            return -1
        buf[0] = <char*>p
        cap[0] = newcap
        return 0

    cdef void _accept_loop(self):
        cdef int nfd
        cdef int fl
        cdef conn_t* c
        with nogil:
            while True:
                nfd = accept(self.lfd, NULL, NULL)
                if nfd < 0:
                    if errno == EINTR:
                        continue
                    if errno == EAGAIN or errno == EWOULDBLOCK:
                        break
                    break

                # fcntl(...) — varargs: берём GIL на время вызовов
                with gil:
                    fl = fcntl(nfd, F_GETFD)
                    fcntl(nfd, F_SETFD, fl | FD_CLOEXEC)
                    fl = fcntl(nfd, F_GETFL)
                    fcntl(nfd, F_SETFL, fl | O_NONBLOCK)

                if self.cur_conns >= self.max_conns:
                    c_close(nfd)
                    continue

                c = self._mk_conn(nfd)
                if c == NULL:
                    c_close(nfd)
                    continue

                self.cur_conns += 1
                self._list_add(c)

    cdef void _handle_read(self, conn_t* c):
        cdef ssize_t n
        cdef size_t free_space
        with nogil:
            while True:
                if c.rlen == c.rcap:
                    if self._ensure_cap(&c.rbuf, &c.rcap, c.rlen + 4096, self.max_bulk) != 0:
                        c.state = CONN_CLOSED
                        return
                free_space = c.rcap - c.rlen
                n = c_read(c.fd, c.rbuf + c.rlen, free_space)
                if n == 0:
                    c.state = CONN_CLOSED
                    return
                if n < 0:
                    if errno == EINTR:
                        continue
                    if errno == EAGAIN or errno == EWOULDBLOCK:
                        break
                    c.state = CONN_CLOSED
                    return
                c.rlen += <size_t>n
                c.last_active_ms = now_ms()
                if c.rlen > self.max_bulk:
                    c.state = CONN_CLOSED
                    return

        self._process_requests(c)

        # >>> Новое: мгновенная запись ответов после парсинга
        if c.state != CONN_CLOSED and c.wlen > c.woff:
            self._handle_write(c)

    cdef void _handle_write(self, conn_t* c):
        if c.wlen == c.woff:
            return
        cdef ssize_t n
        with nogil:
            while c.woff < c.wlen:
                n = c_write(c.fd, c.wbuf + c.woff, c.wlen - c.woff)
                if n < 0:
                    if errno == EINTR:
                        continue
                    if errno == EAGAIN or errno == EWOULDBLOCK:
                        break
                    c.state = CONN_CLOSED
                    return
                c.woff += <size_t>n
                c.last_active_ms = now_ms()
        if c.woff == c.wlen:
            c.woff = 0
            c.wlen = 0

    cdef void _queue_write(self, conn_t* c, const char* data, size_t nbytes):
        if nbytes == 0:
            return
        cdef size_t need = c.wlen + nbytes
        with nogil:
            if self._ensure_cap(&c.wbuf, &c.wcap, need, (<size_t>1<<31)) != 0:
                c.state = CONN_CLOSED
                return
            c_memcpy(c.wbuf + c.wlen, data, nbytes)
            c.wlen += nbytes

    cdef void _process_requests(self, conn_t* c):
        """
        Построчный протокол. 'PING\n' -> 'PONG\n'.
        Если задан on_request_cb(memoryview), он должен вернуть:
          consumed:int, out:bytes|None, close:bool
        """
        cdef size_t i = 0
        cdef size_t start = 0
        cdef size_t pos
        cdef object mv
        cdef size_t cons
        cdef unsigned char NL = 10   # '\n'
        cdef unsigned char CR = 13   # '\r'
        cdef size_t ln

        # Кастомный callback — единым куском
        if self.on_request_cb is not None:
            mv = PyMemoryView_FromMemory(<char*>c.rbuf, <Py_ssize_t>c.rlen, PyBUF_READ)
            try:
                consumed, out, close_flag = self.on_request_cb(mv)
            except Exception:
                c.state = CONN_CLOSED
                return
            if consumed > 0 and consumed <= c.rlen:
                cons = <size_t>consumed
                with nogil:
                    if cons < c.rlen:
                        c_memmove(c.rbuf, c.rbuf + cons, c.rlen - cons)
                    c.rlen -= cons
            if out:
                # безопасно: копируем байты в свой wbuf
                self._queue_write(c, <const char*>out, <size_t>len(out))
            if close_flag:
                c.state = CONN_CLOSED
            return

        # Демонстрационный обработчик: несколько команд в одном буфере
        while i < c.rlen:
            if <unsigned char>c.rbuf[i] == NL:
                pos = i
                # длина строки без '\n' (и возможного '\r')
                ln = pos - start
                if ln > 0 and <unsigned char>c.rbuf[pos - 1] == CR:
                    ln -= 1

                if ln == 4 and c_memcmp(c.rbuf + start, b"PING", 4) == 0:
                    self._queue_write(c, b"PONG\n", 5)
                # здесь можно добавить другие команды

                # следующая строка начинается после '\n'
                start = i + 1
            i += 1

        # Компактируем хвост (частичная команда без '\n')
        if start > 0:
            with nogil:
                if start < c.rlen:
                    c_memmove(c.rbuf, c.rbuf + start, c.rlen - start)
                c.rlen -= start

    cdef int _ensure_pfds_cap(self, int need) noexcept nogil:
        if self.pfds_cap >= need:
            return 0
        cdef int newcap = self.pfds_cap if self.pfds_cap > 0 else 64
        while newcap < need:
            newcap *= 2

        # Атомарное расширение: новые буферы -> копирование -> замена
        cdef void* npfds = c_malloc(newcap * sizeof(pollfd))
        if npfds == NULL:
            return -1
        cdef void* npconns = c_malloc(newcap * sizeof(conn_t*))
        if npconns == NULL:
            c_free(npfds)
            return -1

        if self.pfds != NULL and self.pfds_cap > 0:
            c_memcpy(npfds, self.pfds, self.pfds_cap * sizeof(pollfd))
        if self.pconns != NULL and self.pfds_cap > 0:
            c_memcpy(npconns, self.pconns, self.pfds_cap * sizeof(conn_t*))

        if self.pfds != NULL:
            c_free(self.pfds)
        if self.pconns != NULL:
            c_free(self.pconns)

        self.pfds   = <pollfd*>npfds
        self.pconns = <conn_t**>npconns
        self.pfds_cap = newcap
        return 0

    cpdef run(self):
        cdef uint64_t next_tick_ms = now_ms() + <uint64_t>self.tick_ms
        cdef int nready, i, nitems
        cdef pollfd* pfds
        cdef conn_t** pconns
        cdef conn_t* c
        cdef conn_t* it
        cdef conn_t* nx

        self.running = 1
        try:
            while self.running:
                nitems = self.cur_conns + 1
                with nogil:
                    if self._ensure_pfds_cap(nitems) != 0:
                        pass
                pfds = self.pfds
                pconns = self.pconns

                pfds[0].fd = self.lfd
                pfds[0].events = <short>POLLIN
                pfds[0].revents = 0
                pconns[0] = NULL

                c = self.head
                i = 1
                while c != NULL:
                    pfds[i].fd = c.fd
                    pfds[i].events = <short>POLLIN
                    if c.wlen > c.woff:
                        pfds[i].events = <short>(pfds[i].events | POLLOUT)
                    pfds[i].revents = 0
                    pconns[i] = c
                    c = c.next
                    i += 1

                # Освобождаем GIL на время poll — чтобы не блокировать Python
                with nogil:
                    nready = poll(pfds, nitems, self.tick_ms)
                if nready < 0:
                    if errno == EINTR:
                        continue
                    raise OSError(errno, "poll failed")

                if nready > 0:
                    if pfds[0].revents & (POLLIN | POLLERR | POLLHUP):
                        self._accept_loop()

                    i = 1
                    while i < nitems:
                        c = pconns[i]
                        if c != NULL:
                            if pfds[i].revents & (POLLERR | POLLHUP):
                                c.state = CONN_CLOSED
                            else:
                                if pfds[i].revents & POLLIN:
                                    self._handle_read(c)
                                # >>> Новое: пишем либо по POLLOUT, либо если уже есть что писать
                                if c.state != CONN_CLOSED and ((pfds[i].revents & POLLOUT) or (c.wlen > c.woff)):
                                    self._handle_write(c)

                            if c.state == CONN_CLOSED:
                                with nogil:
                                    self._list_del(c)
                                self.cur_conns -= 1
                                with nogil:
                                    self._free_conn(c)
                        i += 1

                if now_ms() >= next_tick_ms:
                    next_tick_ms += <uint64_t>self.tick_ms
                    if self.on_tick_cb is not None:
                        try:
                            self.on_tick_cb()
                        except Exception:
                            pass

        finally:
            it = self.head
            while it != NULL:
                nx = it.next
                self._free_conn(it)
                it = nx
            self.head = NULL
            if self.pfds != NULL:
                c_free(self.pfds); self.pfds = NULL
            if self.pconns != NULL:
                c_free(self.pconns); self.pconns = NULL
            self.pfds_cap = 0

    @property
    def connections(self) -> int:
        return self.cur_conns

    @property
    def limit(self) -> int:
        return self.max_conns
