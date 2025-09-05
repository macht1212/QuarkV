# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
# distutils: language = c

from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memcmp, memset
from libc.stdint cimport uint64_t
from libc.stddef cimport size_t
from cpython.bytes cimport PyBytes_FromStringAndSize

cdef inline uint64_t fnv1a64(const unsigned char* data, size_t n) nogil:
    cdef uint64_t h = 1469598103934665603ULL  # FNV offset basis
    cdef size_t i
    for i in range(n):
        h ^= data[i]
        h *= 1099511628211ULL  # FNV prime
    return h

cdef struct Entry:
    uint64_t hash
    size_t key_len
    size_t val_len
    unsigned char* key_ptr
    unsigned char* val_ptr
    bint is_tomb

cdef class KV:
    cdef Entry* table
    cdef Py_ssize_t capacity
    cdef Py_ssize_t size      # keys_total
    cdef Py_ssize_t tombs     # tombstones
    cdef size_t mem_bytes
    cdef uint64_t mask        # capacity-1 (при степени двойки)

    def __cinit__(self, Py_ssize_t initial_capacity=1024):
        if initial_capacity < 8:
            initial_capacity = 8
        self.capacity = 1
        while self.capacity < initial_capacity:
            self.capacity <<= 1
        self.mask = <uint64_t>(self.capacity - 1)
        self.table = <Entry*> malloc(self.capacity * sizeof(Entry))
        if self.table == NULL:
            raise MemoryError()
        memset(self.table, 0, self.capacity * sizeof(Entry))
        self.size = 0
        self.tombs = 0
        self.mem_bytes = 0

    def __dealloc__(self):
        cdef Py_ssize_t i
        if self.table != NULL:
            for i in range(self.capacity):
                if self.table[i].key_ptr != NULL:
                    free(self.table[i].key_ptr)
                    free(self.table[i].val_ptr)
            free(self.table)
            self.table = NULL

    cpdef dict stats(self):
        return {
            "capacity": self.capacity,
            "keys_total": self.size,
            "tombstones": self.tombs,
            "mem_bytes": self.mem_bytes,
            "load_factor": (self.size + self.tombs) / float(self.capacity),
        }

    cdef inline void _maybe_resize(self):
        # чисто C-деление во избежание накладных расходов на Python float
        if (<double>(self.size + self.tombs)) / (<double>self.capacity) > 0.8:
            self._resize(self.capacity << 1)

    cdef void _resize(self, Py_ssize_t newcap):
        cdef Entry* oldtab = self.table
        cdef Py_ssize_t oldcap = self.capacity
        cdef Py_ssize_t i
        self.table = <Entry*> malloc(newcap * sizeof(Entry))
        if self.table == NULL:
            self.table = oldtab
            raise MemoryError()
        memset(self.table, 0, newcap * sizeof(Entry))
        self.capacity = newcap
        self.mask = <uint64_t>(newcap - 1)
        self.tombs = 0
        cdef Entry ent
        cdef Py_ssize_t idx, cur, probe, cur_home, cur_probe
        for i in range(oldcap):
            ent = oldtab[i]
            if ent.key_ptr == NULL:
                continue
            idx = <Py_ssize_t>(ent.hash & self.mask)
            probe = 0
            cur = idx
            while True:
                if self.table[cur].key_ptr == NULL:
                    self.table[cur] = ent
                    break
                else:
                    cur_home = <Py_ssize_t>(self.table[cur].hash & self.mask)
                    cur_probe = (cur - cur_home) & <Py_ssize_t>self.mask
                    if cur_probe < probe:
                        ent, self.table[cur] = self.table[cur], ent
                        probe = cur_probe
                cur = (cur + 1) & <Py_ssize_t>self.mask
                probe += 1
        free(oldtab)

    cdef Py_ssize_t _find(self, const unsigned char* kptr, size_t klen, uint64_t h) except -2:
        cdef Py_ssize_t idx = <Py_ssize_t>(h & self.mask)
        cdef Py_ssize_t i = idx
        cdef Py_ssize_t probe = 0
        cdef Py_ssize_t cur_home, cur_probe
        cdef Entry* tab = self.table
        cdef Py_ssize_t capm = <Py_ssize_t>self.mask
        while True:
            if tab[i].key_ptr == NULL:
                if not tab[i].is_tomb:
                    return -1
            else:
                if tab[i].hash == h and tab[i].key_len == klen and memcmp(tab[i].key_ptr, kptr, klen) == 0:
                    return i
                cur_home = <Py_ssize_t>(tab[i].hash & self.mask)
                cur_probe = (i - cur_home) & capm
                if cur_probe < probe:
                    return -1
            i = (i + 1) & capm
            probe += 1
            if probe > self.capacity:
                return -1

    def __len__(self):
        return self.size

    cpdef bint exists(self, bytes key):
        cdef const unsigned char* kptr = <const unsigned char*> key
        cdef size_t klen = <size_t> len(key)
        cdef uint64_t h = fnv1a64(kptr, klen)
        return self._find(kptr, klen, h) >= 0

    cpdef object get(self, bytes key):
        cdef const unsigned char* kptr = <const unsigned char*> key
        cdef size_t klen = <size_t> len(key)
        cdef uint64_t h = fnv1a64(kptr, klen)
        cdef Py_ssize_t pos = self._find(kptr, klen, h)
        if pos < 0:
            return None
        cdef Entry* e = &self.table[pos]
        return PyBytes_FromStringAndSize(<char*> e.val_ptr, <Py_ssize_t> e.val_len)

    cpdef bint delete(self, bytes key):
        cdef const unsigned char* kptr = <const unsigned char*> key
        cdef size_t klen = <size_t> len(key)
        cdef uint64_t h = fnv1a64(kptr, klen)
        cdef Py_ssize_t pos = self._find(kptr, klen, h)
        if pos < 0:
            return False
        cdef Entry* e = &self.table[pos]
        if e.key_ptr != NULL:
            self.mem_bytes -= e.key_len + e.val_len
            free(e.key_ptr)
            free(e.val_ptr)
            e.key_ptr = NULL
            e.val_ptr = NULL
            e.key_len = 0
            e.val_len = 0
            e.hash = 0
            e.is_tomb = 1
            self.size -= 1
            self.tombs += 1
            return True
        return False

    cpdef void set(self, bytes key, bytes value):
        cdef const unsigned char* ksrc = <const unsigned char*> key
        cdef size_t klen = <size_t> len(key)
        cdef const unsigned char* vsrc = <const unsigned char*> value
        cdef size_t vlen = <size_t> len(value)
        cdef uint64_t h = fnv1a64(ksrc, klen)

        cdef Py_ssize_t pos = self._find(ksrc, klen, h)
        cdef Entry* e
        if pos >= 0:
            e = &self.table[pos]
            self.mem_bytes -= e.val_len
            free(e.val_ptr)
            e.val_ptr = <unsigned char*> malloc(vlen)
            if e.val_ptr == NULL:
                self.mem_bytes += e.val_len
                raise MemoryError()
            memcpy(e.val_ptr, vsrc, vlen)
            e.val_len = vlen
            self.mem_bytes += vlen
            return

        self._maybe_resize()

        cdef Entry cand
        cand.hash = h
        cand.key_len = klen
        cand.val_len = vlen
        cand.is_tomb = 0
        cand.key_ptr = <unsigned char*> malloc(klen)
        if cand.key_ptr == NULL:
            raise MemoryError()
        memcpy(cand.key_ptr, ksrc, klen)
        cand.val_ptr = <unsigned char*> malloc(vlen)
        if cand.val_ptr == NULL:
            free(cand.key_ptr)
            raise MemoryError()
        memcpy(cand.val_ptr, vsrc, vlen)

        cdef Py_ssize_t idx = <Py_ssize_t>(h & self.mask)
        cdef Py_ssize_t i = idx
        cdef Py_ssize_t probe = 0
        cdef Py_ssize_t cur_home, cur_probe

        while True:
            e = &self.table[i]
            if e.key_ptr == NULL:
                if e.is_tomb:
                    self.tombs -= 1
                    e.is_tomb = 0
                e[0] = cand
                self.size += 1
                self.mem_bytes += klen + vlen
                break
            else:
                cur_home = <Py_ssize_t>(e.hash & <uint64_t>self.mask)
                cur_probe = (i - cur_home) & <Py_ssize_t>self.mask
                if cur_probe < probe:
                    cand, e[0] = e[0], cand
                    probe = cur_probe
            i = (i + 1) & <Py_ssize_t>self.mask
            probe += 1

    def __repr__(self):
        s = self.stats()
        return f"KV(capacity={s['capacity']}, size={s['keys_total']}, tombstones={s['tombstones']}, load_factor={s['load_factor']:.3f})"

def selftest(int n = 100000, int klen = 16, int vlen = 32, int seed = 12345):
    import os, time, random
    rng = random.Random(seed)
    kv = KV(initial_capacity=max(8, n // 2))
    keys = [os.urandom(klen) for _ in range(n)]
    vals = [os.urandom(vlen) for _ in range(n)]

    t0 = time.perf_counter()
    for i in range(n):
        kv.set(keys[i], vals[i])
    t1 = time.perf_counter()

    ok = True
    for i in range(n):
        got = kv.get(keys[i])
        if got != vals[i]:
            ok = False
            break
    t2 = time.perf_counter()

    return {
        "ok": ok,
        "stats": kv.stats(),
        "insert_avg_us": (t1 - t0) * 1e6 / n,
        "get_avg_us": (t2 - t1) * 1e6 / n,
    }
