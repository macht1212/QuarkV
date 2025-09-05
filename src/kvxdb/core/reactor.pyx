# distutils: language = c
# cython: boundscheck=False, wraparound=False, cdivision=True, nonecheck=False, infer_types=True

from libc.stdint cimport uint32_t, uint64_t, size_t
from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset, memmove, strlen, memcmp
from libc.time cimport time
from libc.errno cimport errno, EINTR, EAGAIN, EWOULDBLOCK
from libc.unistd cimport close, read, write
from libc.fcntl cimport fcntl, F_GETFL, F_SETFL, F_GETFD, F_SETFD, O_NONBLOCK, FD_CLOEXEC
from cpython.memoryview cimport PyMemoryView_FromMemory
from cpython.buffer cimport PyBUF_READ

# --- networking headers ---
cdef extern from "<arpa/inet.h>":
    unsigned short htons(unsigned short hostshort)
    unsigned int inet_addr(const char* cp)

cdef extern from "<netinet/in.h>":
    ctypedef unsigned short in_port_t
    ctypedef unsigned int in_addr_t
    cdef struct in_addr:
        in_addr_t s_addr
    cdef struct sockaddr_in:
        unsigned short  sin_family
        in_port_t       sin_port
        in_addr         sin_addr
        char            sin_zero[8]

cdef extern from "<sys/socket.h>":
    # forward-declare sockaddr (поля не нужны)
    ctypedef struct sockaddr:
        pass

    ctypedef unsigned int socklen_t

    int socket(int domain, int type, int protocol)
    int bind(int sockfd, const sockaddr* addr, unsigned int addrlen)
    int listen(int sockfd, int backlog)
    int setsockopt(int sockfd, int level, int optname, const void *optval, unsigned int optlen)
    int accept(int sockfd, void *addr, unsigned int *addrlen)
    int shutdown(int sockfd, int how)

    cdef int AF_INET
    cdef int SOCK_STREAM
    cdef int SOL_SOCKET
    cdef int SO_REUSEADDR
    cdef int SO_REUSEPORT   # может отсутствовать в рантайме — ошибки игнорируем

# --- poll(2) cross-platform ---
cdef extern from "<poll.h>":
    cdef struct pollfd:
        int   fd
        short events
        short revents

    int poll(pollfd* fds, int nfds, int timeout)

    cdef int POLLIN
    cdef int POLLOUT
    cdef int POLLERR
    cdef int POLLHUP

# --- helpers ---

cdef inline uint64_t now_ms() nogil:
    # Грубая миллисекундная метка на базе time(NULL) — точность ~1 сек.
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
    # двусвязный список активных соединений
    conn_t*    next
    conn_t*    prev

cdef class Reactor:
    cdef:
        # listening socket
        int lfd

        # список активных соединений
        conn_t* head
        int     cur_conns
        int     max_conns

        # лимиты
        size_t  max_bulk
        int     tick_ms

        # пользовательские колбэки
        object on_request_cb   # (memoryview) -> (consumed:int, out:bytes|None, close:bool)
        object on_tick_cb

        int backlog
        int running

        # рабочие массивы для poll(2), реюзим и растим по мере нужды
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
        """
        Кроссплатформенный (Linux/macOS) неблокирующий TCP-сервер на poll(2).

        - tick_ms: период фонового тика (>=10 мс).
        - max_conns: лимит одновременно открытых соединений.
        - max_bulk: защита от слишком больших запросов.
        """
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

        # CLOEXEC + NONBLOCK через fcntl (кроссплатформенно)
        cdef int flags = fcntl(self.lfd, F_GETFD)
        fcntl(self.lfd, F_SETFD, flags | FD_CLOEXEC)
        flags = fcntl(self.lfd, F_GETFL)
        fcntl(self.lfd, F_SETFL, flags | O_NONBLOCK)

        # SO_REUSEADDR (и SO_REUSEPORT, если поддерживается ядром — ошибки игнорируем)
        if setsockopt(self.lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) != 0:
            self._cleanup()
            raise OSError(errno, "setsockopt SO_REUSEADDR failed")
        setsockopt(self.lfd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(int))

        # bind()
        memset(&addr, 0, sizeof(addr))
        addr.sin_family = AF_INET
        addr.sin_port = htons(<unsigned short>port)
        cdef const char* host_c = host
        addr.sin_addr.s_addr = inet_addr(host_c)

        if bind(self.lfd, <const sockaddr*>&addr, sizeof(addr)) != 0:
            self._cleanup()
            raise OSError(errno, "bind failed")

        # listen()
        if listen(self.lfd, self.backlog) != 0:
            self._cleanup()
            raise OSError(errno, "listen failed")

    cdef void _cleanup(self) nogil:
        if self.lfd >= 0:
            close(self.lfd)
        self.lfd = -1

    def close(self):
        self.stop()
        if self.lfd >= 0:
            close(self.lfd)
            self.lfd = -1

    def stop(self):
        self.running = 0

    # --- conn list management ---
    cdef inline void _list_add(self, conn_t* c) nogil:
        c.prev = NULL
        c.next = self.head
        if self.head != NULL:
            self.head.prev = c
        self.head = c

    cdef inline void _list_del(self, conn_t* c) nogil:
        if c.prev != NULL:
            c.prev.next = c.next
        else:
            self.head = c.next
        if c.next != NULL:
            c.next.prev = c.prev
        c.prev = c.next = NULL

    cdef inline conn_t* _mk_conn(self, int fd) nogil:
        cdef conn_t* c = <conn_t*>malloc(sizeof(conn_t))
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
        c.rbuf = <char*>malloc(c.rcap)
        c.wbuf = <char*>malloc(c.wcap)
        c.prev = NULL
        c.next = NULL
        if (c.rbuf == NULL) or (c.wbuf == NULL):
            if c.rbuf: free(c.rbuf)
            if c.wbuf: free(c.wbuf)
            free(c)
            return NULL
        return c

    cdef inline void _free_conn(self, conn_t* c) nogil:
        if c == NULL:
            return
        if c.fd >= 0:
            close(c.fd)
            c.fd = -1
        if c.rbuf != NULL:
            free(c.rbuf); c.rbuf = NULL
        if c.wbuf != NULL:
            free(c.wbuf); c.wbuf = NULL
        free(c)

    cdef inline int _ensure_cap(self, char** buf, size_t* cap, size_t need, size_t hard_max) nogil:
        if *cap >= need:
            return 0
        cdef size_t newcap = *cap
        while newcap < need:
            newcap = newcap * 2
            if newcap > hard_max:
                newcap = hard_max
                break
        if newcap < need:
            return -1
        # ВАЖНО: разыменование как buf[0], а не <void*>*buf
        cdef void* p = realloc(buf[0], newcap)
        if p == NULL:
            return -1
        *buf = <char*>p
        *cap = newcap
        return 0

    cdef void _accept_loop(self):
        cdef int nfd
        with nogil:
            while True:
                nfd = accept(self.lfd, NULL, NULL)
                if nfd < 0:
                    if errno == EINTR:
                        continue
                    if errno == EAGAIN or errno == EWOULDBLOCK:
                        break
                    # иные ошибки — выходим из accept-loop
                    break

                # настроим O_NONBLOCK + CLOEXEC на клиентском сокете
                cdef int fl = fcntl(nfd, F_GETFD)
                fcntl(nfd, F_SETFD, fl | FD_CLOEXEC)
                fl = fcntl(nfd, F_GETFL)
                fcntl(nfd, F_SETFL, fl | O_NONBLOCK)

                if self.cur_conns >= self.max_conns:
                    close(nfd)
                    continue

                cdef conn_t* c = self._mk_conn(nfd)
                if c == NULL:
                    close(nfd)
                    continue

                self.cur_conns += 1
                self._list_add(c)

    cdef void _handle_read(self, conn_t* c):
        cdef Py_ssize_t n
        cdef size_t free_space
        with nogil:
            while True:
                if c.rlen == c.rcap:
                    if self._ensure_cap(&c.rbuf, &c.rcap, c.rlen + 4096, self.max_bulk) != 0:
                        c.state = CONN_CLOSED
                        return
                free_space = c.rcap - c.rlen
                n = read(c.fd, c.rbuf + c.rlen, free_space)
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

    cdef void _handle_write(self, conn_t* c):
        if c.wlen == c.woff:
            return
        cdef Py_ssize_t n
        with nogil:
            while c.woff < c.wlen:
                n = write(c.fd, c.wbuf + c.woff, c.wlen - c.woff)
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
            # POLLOUT интерес будет снят на следующей итерации, когда мы перестроим pfds

    cdef void _queue_write(self, conn_t* c, const char* data, size_t nbytes):
        if nbytes == 0:
            return
        cdef size_t need = c.wlen + nbytes
        with nogil:
            if self._ensure_cap(&c.wbuf, &c.wcap, need, (<size_t>1<<31)) != 0:
                c.state = CONN_CLOSED
                return
            memcpy(c.wbuf + c.wlen, data, nbytes)
            c.wlen += nbytes
            # интерес POLLOUT появится в следующем билде pfds

    cdef void _process_requests(self, conn_t* c):
        """
        По умолчанию: строковый протокол по '\\n'. 'PING\\n' -> 'PONG\\n'.
        Если задан on_request_cb(memoryview), он должен вернуть:
          consumed:int, out:bytes|None, close:bool
        """
        cdef size_t i = 0
        cdef size_t start = 0
        cdef size_t pos

        if self.on_request_cb is not None:
            # zero-copy memoryview на rbuf
            cdef object mv = PyMemoryView_FromMemory(<char*>c.rbuf, <Py_ssize_t>c.rlen, PyBUF_READ)
            try:
                consumed, out, close_flag = self.on_request_cb(mv)
            except Exception:
                c.state = CONN_CLOSED
                return
            if consumed > 0 and consumed <= c.rlen:
                with nogil:
                    if consumed < c.rlen:
                        memmove(c.rbuf, c.rbuf + consumed, c.rlen - consumed)
                    c.rlen -= consumed
            if out:
                self._queue_write(c, out, len(out))
            if close_flag:
                c.state = CONN_CLOSED
            return

        # демо-протокол: ищем '\n' и отвечаем на PING
        while i < c.rlen:
            if c.rbuf[i] == b'\n'[0]:
                pos = i
                if (pos - start + 1) == 5 and memcmp(c.rbuf + start, b"PING", 4) == 0:
                    self._queue_write(c, b"PONG\n", 5)
                start = i + 1
            i += 1

        if start > 0:
            with nogil:
                if start < c.rlen:
                    memmove(c.rbuf, c.rbuf + start, c.rlen - start)
                c.rlen -= start

    cdef int _ensure_pfds_cap(self, int need) nogil:
        if self.pfds_cap >= need:
            return 0
        cdef int newcap = self.pfds_cap if self.pfds_cap > 0 else 64
        while newcap < need:
            newcap *= 2
        cdef void* npfds = realloc(self.pfds, newcap * sizeof(pollfd))
        if npfds == NULL:
            return -1
        self.pfds = <pollfd*>npfds
        cdef void* npconns = realloc(self.pconns, newcap * sizeof(conn_t*))
        if npconns == NULL:
            return -1
        self.pconns = <conn_t**>npconns
        self.pfds_cap = newcap
        return 0

    cpdef run(self):
        """
        Главный цикл на poll(2). Останавливается вызовом stop().
        """
        cdef uint64_t next_tick_ms = now_ms() + <uint64_t>self.tick_ms
        cdef int nready, i, nitems
        cdef pollfd* pfds
        cdef conn_t** pconns

        self.running = 1
        try:
            while self.running:
                # 1) собрать pfds: [lfd] + все активные соединения
                nitems = self.cur_conns + 1
                with nogil:
                    if self._ensure_pfds_cap(nitems) != 0:
                        pass
                pfds = self.pfds
                pconns = self.pconns

                # слушающий fd
                pfds[0].fd = self.lfd
                pfds[0].events = <short>POLLIN
                pfds[0].revents = 0
                pconns[0] = NULL  # индекс 0 — listener

                # клиенты
                cdef conn_t* c = self.head
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

                # 2) ждём события не больше tick_ms миллисекунд
                nready = poll(pfds, nitems, self.tick_ms)
                if nready < 0:
                    if errno == EINTR:
                        continue
                    raise OSError(errno, "poll failed")

                # 3) обработка событий
                if nready > 0:
                    # 3.1) listener
                    if pfds[0].revents & (POLLIN | POLLERR | POLLHUP):
                        self._accept_loop()

                    # 3.2) клиенты
                    i = 1
                    while i < nitems:
                        c = pconns[i]
                        if c != NULL:
                            if pfds[i].revents & (POLLERR | POLLHUP):
                                c.state = CONN_CLOSED
                            else:
                                if pfds[i].revents & POLLIN:
                                    self._handle_read(c)
                                if c.state != CONN_CLOSED and (pfds[i].revents & POLLOUT):
                                    self._handle_write(c)

                            if c.state == CONN_CLOSED:
                                with nogil:
                                    self._list_del(c)
                                self.cur_conns -= 1
                                with nogil:
                                    self._free_conn(c)
                        i += 1

                # 4) тикер
                if now_ms() >= next_tick_ms:
                    next_tick_ms += <uint64_t>self.tick_ms
                    if self.on_tick_cb is not None:
                        try:
                            self.on_tick_cb()
                        except Exception:
                            pass

        finally:
            # зачистка всех оставшихся коннектов и массивов
            cdef conn_t* it = self.head
            cdef conn_t* nx
            while it != NULL:
                nx = it.next
                self._free_conn(it)
                it = nx
            self.head = NULL
            if self.pfds != NULL:
                free(self.pfds); self.pfds = NULL
            if self.pconns != NULL:
                free(self.pconns); self.pconns = NULL
            self.pfds_cap = 0

    @property
    def connections(self) -> int:
        return self.cur_conns

    @property
    def limit(self) -> int:
        return self.max_conns
