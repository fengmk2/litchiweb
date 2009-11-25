#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""non-blocking memcache base on memcache.py
"""
import socket
import sys

from litchi.utils import memcache
from litchi.socketwrap import Socket



class AsyncClient(memcache.Client):
    
    def set_servers(self, servers):
        """
        Set the pool of servers used by this client.

        @param servers: an array of servers.
        Servers can be passed in two forms:
            1. Strings of the form C{"host:port"}, which implies a default weight of 1.
            2. Tuples of the form C{("host:port", weight)}, where C{weight} is
            an integer weight value.
        """
        self.servers = [_AsyncHost(s, self.debuglog) for s in servers]
        self._init_buckets()
    
    def get_stats(self):
        '''Get statistics from each of the servers.

        @return: A list of tuples ( server_identifier, stats_dictionary ).
            The dictionary contains a number of name/value pairs specifying
            the name of the status field and the string value associated with
            it.  The values are not converted from strings.
        '''
        data = []
        for s in self.servers:
            if not s.connect(): continue
            if s.family == socket.AF_INET:
                name = '%s:%s (%s)' % ( s.ip, s.port, s.weight )
            else:
                name = 'unix:%s (%s)' % ( s.address, s.weight )
            yield s.send_cmd('stats')
            serverData = {}
            data.append(( name, serverData ))
            readline = s.readline
            while 1:
                line = yield readline()
                if not line or line.strip() == 'END': break
                stats = line.split(' ', 2)
                serverData[stats[1]] = stats[2]

        yield data

    def get_slabs(self):
        data = []
        for s in self.servers:
            if not s.connect(): continue
            if s.family == socket.AF_INET:
                name = '%s:%s (%s)' % ( s.ip, s.port, s.weight )
            else:
                name = 'unix:%s (%s)' % ( s.address, s.weight )
            serverData = {}
            data.append(( name, serverData ))
            yield s.send_cmd('stats items')
            readline = s.readline
            while 1:
                line = yield readline()
                if not line or line.strip() == 'END': break
                item = line.split(' ', 2)
                #0 = STAT, 1 = ITEM, 2 = Value
                slab = item[1].split(':', 2)
                #0 = items, 1 = Slab #, 2 = Name
                if not serverData.has_key(slab[1]):
                    serverData[slab[1]] = {}
                serverData[slab[1]][slab[2]] = item[2]
        yield data

    def flush_all(self):
        'Expire all data currently in the memcache servers.'
        for s in self.servers:
            if not s.connect(): continue
            yield s.send_cmd('flush_all')
            yield s.expect("OK")
            
    def delete_multi(self, keys, time=0, key_prefix=''):
        '''
        Delete multiple keys in the memcache doing just one query.

        >>> notset_keys = mc.set_multi({'key1' : 'val1', 'key2' : 'val2'})
        >>> mc.get_multi(['key1', 'key2']) == {'key1' : 'val1', 'key2' : 'val2'}
        1
        >>> mc.delete_multi(['key1', 'key2'])
        1
        >>> mc.get_multi(['key1', 'key2']) == {}
        1


        This method is recommended over iterated regular L{delete}s as it reduces total latency, since
        your app doesn't have to wait for each round-trip of L{delete} before sending
        the next one.

        @param keys: An iterable of keys to clear
        @param time: number of seconds any subsequent set / update commands should fail. Defaults to 0 for no delay.
        @param key_prefix:  Optional string to prepend to each key when sending to memcache.
            See docs for L{get_multi} and L{set_multi}.

        @return: 1 if no failure in communication with any memcacheds.
        @rtype: int

        '''

        self._statlog('delete_multi')

        server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(keys, key_prefix)

        # send out all requests on each server before reading anything
        dead_servers = []

        rc = 1
        for server in server_keys.iterkeys():
            bigcmd = []
            write = bigcmd.append
            if time != None:
                for key in server_keys[server]: # These are mangled keys
                    write("delete %s %d\r\n" % (key, time))
            else:
                for key in server_keys[server]: # These are mangled keys
                    write("delete %s\r\n" % key)
            try:
                yield server.send_cmds(''.join(bigcmd))
            except socket.error, msg:
                rc = 0
                if isinstance(msg, tuple): msg = msg[1]
                server.mark_dead(msg)
                dead_servers.append(server)

        # if any servers died on the way, don't expect them to respond.
        for server in dead_servers:
            del server_keys[server]

        notstored = [] # original keys.
        for server, keys in server_keys.iteritems():
            try:
                for key in keys:
                    yield server.expect("DELETED")
            except socket.error, msg:
                if isinstance(msg, tuple): msg = msg[1]
                server.mark_dead(msg)
                rc = 0
        yield rc

    def delete(self, key, time=0):
        '''Deletes a key from the memcache.

        @return: Nonzero on success.
        @param time: number of seconds any subsequent set / update commands should fail. Defaults to 0 for no delay.
        @rtype: int
        '''
        memcache.check_key(key)
        server, key = self._get_server(key)
        if not server:
            yield 0
        self._statlog('delete')
        if time != None:
            cmd = "delete %s %d" % (key, time)
        else:
            cmd = "delete %s" % key

        try:
            yield server.send_cmd(cmd)
            yield server.expect("DELETED")
        except socket.error, msg:
            if isinstance(msg, tuple): msg = msg[1]
            server.mark_dead(msg)
            yield 0
        yield 1

    def incr(self, key, delta=1):
        """
        Sends a command to the server to atomically increment the value for C{key} by
        C{delta}, or by 1 if C{delta} is unspecified.  Returns None if C{key} doesn't
        exist on server, otherwise it returns the new value after incrementing.

        Note that the value for C{key} must already exist in the memcache, and it
        must be the string representation of an integer.

        >>> mc.set("counter", "20")  # returns 1, indicating success
        1
        >>> mc.incr("counter")
        21
        >>> mc.incr("counter")
        22

        Overflow on server is not checked.  Be aware of values approaching
        2**32.  See L{decr}.

        @param delta: Integer amount to increment by (should be zero or greater).
        @return: New value after incrementing.
        @rtype: int
        """
        yield self._incrdecr("incr", key, delta)

    def decr(self, key, delta=1):
        """
        Like L{incr}, but decrements.  Unlike L{incr}, underflow is checked and
        new values are capped at 0.  If server value is 1, a decrement of 2
        returns 0, not -1.

        @param delta: Integer amount to decrement by (should be zero or greater).
        @return: New value after decrementing.
        @rtype: int
        """
        yield self._incrdecr("decr", key, delta)

    def _incrdecr(self, cmd, key, delta):
        memcache.check_key(key)
        server, key = self._get_server(key)
        if not server:
            yield 0
        self._statlog(cmd)
        cmd = "%s %s %d" % (cmd, key, delta)
        try:
            yield server.send_cmd(cmd)
            line = yield server.readline()
            yield int(line)
        except socket.error, msg:
            if isinstance(msg, tuple): msg = msg[1]
            server.mark_dead(msg)
            yield None

    def add(self, key, val, time = 0, min_compress_len = 0):
        '''
        Add new key with value.

        Like L{set}, but only stores in memcache if the key doesn't already exist.

        @return: Nonzero on success.
        @rtype: int
        '''
        return self._set("add", key, val, time, min_compress_len)

    def append(self, key, val, time=0, min_compress_len=0):
        '''Append the value to the end of the existing key's value.

        Only stores in memcache if key already exists.
        Also see L{prepend}.

        @return: Nonzero on success.
        @rtype: int
        '''
        yield self._set("append", key, val, time, min_compress_len)

    def prepend(self, key, val, time=0, min_compress_len=0):
        '''Prepend the value to the beginning of the existing key's value.

        Only stores in memcache if key already exists.
        Also see L{append}.

        @return: Nonzero on success.
        @rtype: int
        '''
        yield self._set("prepend", key, val, time, min_compress_len)

    def replace(self, key, val, time=0, min_compress_len=0):
        '''Replace existing key with value.

        Like L{set}, but only stores in memcache if the key already exists.
        The opposite of L{add}.

        @return: Nonzero on success.
        @rtype: int
        '''
        yield self._set("replace", key, val, time, min_compress_len)

    def set(self, key, val, time=0, min_compress_len=0):
        '''Unconditionally sets a key to a given value in the memcache.

        The C{key} can optionally be an tuple, with the first element
        being the server hash value and the second being the key.
        If you want to avoid making this module calculate a hash value.
        You may prefer, for example, to keep all of a given user's objects
        on the same memcache server, so you could use the user's unique
        id as the hash value.

        @return: Nonzero on success.
        @rtype: int
        @param time: Tells memcached the time which this value should expire, either
        as a delta number of seconds, or an absolute unix time-since-the-epoch
        value. See the memcached protocol docs section "Storage Commands"
        for more info on <exptime>. We default to 0 == cache forever.
        @param min_compress_len: The threshold length to kick in auto-compression
        of the value using the zlib.compress() routine. If the value being cached is
        a string, then the length of the string is measured, else if the value is an
        object, then the length of the pickle result is measured. If the resulting
        attempt at compression yeilds a larger string than the input, then it is
        discarded. For backwards compatability, this parameter defaults to 0,
        indicating don't ever try to compress.
        '''
        yield self._set("set", key, val, time, min_compress_len)

    def set_multi(self, mapping, time=0, key_prefix='', min_compress_len=0):
        '''
        Sets multiple keys in the memcache doing just one query.

        >>> notset_keys = mc.set_multi({'key1' : 'val1', 'key2' : 'val2'})
        >>> mc.get_multi(['key1', 'key2']) == {'key1' : 'val1', 'key2' : 'val2'}
        1


        This method is recommended over regular L{set} as it lowers the number of
        total packets flying around your network, reducing total latency, since
        your app doesn't have to wait for each round-trip of L{set} before sending
        the next one.

        @param mapping: A dict of key/value pairs to set.
        @param time: Tells memcached the time which this value should expire, either
        as a delta number of seconds, or an absolute unix time-since-the-epoch
        value. See the memcached protocol docs section "Storage Commands"
        for more info on <exptime>. We default to 0 == cache forever.
        @param key_prefix:  Optional string to prepend to each key when sending to memcache. Allows you to efficiently stuff these keys into a pseudo-namespace in memcache:
            >>> notset_keys = mc.set_multi({'key1' : 'val1', 'key2' : 'val2'}, key_prefix='subspace_')
            >>> len(notset_keys) == 0
            True
            >>> mc.get_multi(['subspace_key1', 'subspace_key2']) == {'subspace_key1' : 'val1', 'subspace_key2' : 'val2'}
            True

            Causes key 'subspace_key1' and 'subspace_key2' to be set. Useful in conjunction with a higher-level layer which applies namespaces to data in memcache.
            In this case, the return result would be the list of notset original keys, prefix not applied.

        @param min_compress_len: The threshold length to kick in auto-compression
        of the value using the zlib.compress() routine. If the value being cached is
        a string, then the length of the string is measured, else if the value is an
        object, then the length of the pickle result is measured. If the resulting
        attempt at compression yeilds a larger string than the input, then it is
        discarded. For backwards compatability, this parameter defaults to 0,
        indicating don't ever try to compress.
        @return: List of keys which failed to be stored [ memcache out of memory, etc. ].
        @rtype: list

        '''

        self._statlog('set_multi')



        server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(mapping.iterkeys(), key_prefix)

        # send out all requests on each server before reading anything
        dead_servers = []

        for server in server_keys.iterkeys():
            bigcmd = []
            write = bigcmd.append
            try:
                for key in server_keys[server]: # These are mangled keys
                    store_info = self._val_to_store_info(mapping[prefixed_to_orig_key[key]], min_compress_len)
                    write("set %s %d %d %d\r\n%s\r\n" % (key, store_info[0], time, store_info[1], store_info[2]))
                yield server.send_cmds(''.join(bigcmd))
            except socket.error, msg:
                if isinstance(msg, tuple): msg = msg[1]
                server.mark_dead(msg)
                dead_servers.append(server)

        # if any servers died on the way, don't expect them to respond.
        for server in dead_servers:
            del server_keys[server]

        #  short-circuit if there are no servers, just return all keys
        if not server_keys: yield mapping.keys()

        notstored = [] # original keys.
        for server, keys in server_keys.iteritems():
            try:
                for key in keys:
                    line = yield server.readline()
                    if line == 'STORED':
                        continue
                    else:
                        notstored.append(prefixed_to_orig_key[key]) #un-mangle.
            except (memcache._Error, socket.error), msg:
                if isinstance(msg, tuple): msg = msg[1]
                server.mark_dead(msg)
        yield notstored

    def _set(self, cmd, key, val, time, min_compress_len = 0):
        memcache.check_key(key)
        server, key = self._get_server(key)
        if not server:
            yield 0

        self._statlog(cmd)

        store_info = self._val_to_store_info(val, min_compress_len)
        if not store_info: yield 0

        fullcmd = "%s %s %d %d %d\r\n%s" % (cmd, key, store_info[0], time, store_info[1], store_info[2])
        try:
            yield server.send_cmd(fullcmd)
            yield ((yield server.expect("STORED")) == "STORED")
        except socket.error, msg:
            if isinstance(msg, tuple): msg = msg[1]
            server.mark_dead(msg)
        yield 0

    def get(self, key):
        '''Retrieves a key from the memcache.

        @return: The value or None.
        '''
        memcache.check_key(key)
        server, key = self._get_server(key)
        if not server:
            yield None

        self._statlog('get')

        try:
            yield server.send_cmd("get %s" % key)
            rkey, flags, rlen, = yield self._expectvalue(server)
            if not rkey:
                yield None
            value = yield self._recv_value(server, flags, rlen)
            yield server.expect("END")
        except (memcache._Error, socket.error), msg:
            if isinstance(msg, tuple): msg = msg[1]
            server.mark_dead(msg)
            yield None
        yield value

    def get_multi(self, keys, key_prefix=''):
        '''
        Retrieves multiple keys from the memcache doing just one query.

        >>> success = mc.set("foo", "bar")
        >>> success = mc.set("baz", 42)
        >>> mc.get_multi(["foo", "baz", "foobar"]) == {"foo": "bar", "baz": 42}
        1
        >>> mc.set_multi({'k1' : 1, 'k2' : 2}, key_prefix='pfx_') == []
        1

        This looks up keys 'pfx_k1', 'pfx_k2', ... . Returned dict will just have unprefixed keys 'k1', 'k2'.
        >>> mc.get_multi(['k1', 'k2', 'nonexist'], key_prefix='pfx_') == {'k1' : 1, 'k2' : 2}
        1

        get_mult [ and L{set_multi} ] can take str()-ables like ints / longs as keys too. Such as your db pri key fields.
        They're rotored through str() before being passed off to memcache, with or without the use of a key_prefix.
        In this mode, the key_prefix could be a table name, and the key itself a db primary key number.

        >>> mc.set_multi({42: 'douglass adams', 46 : 'and 2 just ahead of me'}, key_prefix='numkeys_') == []
        1
        >>> mc.get_multi([46, 42], key_prefix='numkeys_') == {42: 'douglass adams', 46 : 'and 2 just ahead of me'}
        1

        This method is recommended over regular L{get} as it lowers the number of
        total packets flying around your network, reducing total latency, since
        your app doesn't have to wait for each round-trip of L{get} before sending
        the next one.

        See also L{set_multi}.

        @param keys: An array of keys.
        @param key_prefix: A string to prefix each key when we communicate with memcache.
            Facilitates pseudo-namespaces within memcache. Returned dictionary keys will not have this prefix.
        @return:  A dictionary of key/value pairs that were available. If key_prefix was provided, the keys in the retured dictionary will not have it present.

        '''

        self._statlog('get_multi')

        server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(keys, key_prefix)

        # send out all requests on each server before reading anything
        dead_servers = []
        for server in server_keys.iterkeys():
            try:
                yield server.send_cmd("get %s" % " ".join(server_keys[server]))
            except socket.error, msg:
                if isinstance(msg, tuple): msg = msg[1]
                server.mark_dead(msg)
                dead_servers.append(server)

        # if any servers died on the way, don't expect them to respond.
        for server in dead_servers:
            del server_keys[server]

        retvals = {}
        for server in server_keys.iterkeys():
            try:
                line = yield server.readline()
                while line and line != 'END':
                    rkey, flags, rlen = yield self._expectvalue(server, line)
                    #  Bo Yang reports that this can sometimes be None
                    if rkey is not None:
                        val = yield self._recv_value(server, flags, rlen)
                        retvals[prefixed_to_orig_key[rkey]] = val   # un-prefix returned key.
                    line = yield server.readline()
            except (memcache._Error, socket.error), msg:
                if isinstance(msg, tuple): msg = msg[1]
                server.mark_dead(msg)
        yield retvals

    def _expectvalue(self, server, line=None):
        if not line:
            line = yield server.readline()

        if line[:5] == 'VALUE':
            resp, rkey, flags, len = line.split()
            flags = int(flags)
            rlen = int(len)
            yield (rkey, flags, rlen)
        else:
            yield (None, None, None)

    def _recv_value(self, server, flags, rlen):
        rlen += 2 # include \r\n
        buf = yield server.recv(rlen)
        if len(buf) != rlen:
            raise memcache._Error("received %d bytes when expecting %d" % (len(buf), rlen))

        if len(buf) == rlen:
            buf = buf[:-2]  # strip \r\n

        if flags & memcache.Client._FLAG_COMPRESSED:
            buf = memcache.decompress(buf)


        if  flags == 0 or flags == memcache.Client._FLAG_COMPRESSED:
            # Either a bare string or a compressed string now decompressed...
            val = buf
        elif flags & memcache.Client._FLAG_INTEGER:
            val = int(buf)
        elif flags & memcache.Client._FLAG_LONG:
            val = long(buf)
        elif flags & memcache.Client._FLAG_PICKLE:
            try:
                file = memcache.StringIO(buf)
                unpickler = self.unpickler(file)
                if self.persistent_load:
                    unpickler.persistent_load = self.persistent_load
                val = unpickler.load()
            except Exception, e:
                self.debuglog('Pickle error: %s\n' % e)
                val = None
        else:
            self.debuglog("unknown flags on get: %x\n" % flags)

        yield val

class _AsyncHost(memcache._Host):
    
    def _get_socket(self):
        if self._check_dead():
            return None
        if self.socket:
            return self.socket
        s = Socket(self.family, socket.SOCK_STREAM)
        if hasattr(s, 'settimeout'): 
            s.settimeout(self._SOCKET_TIMEOUT)
        try:
            s.connect(self.address)
        except socket.timeout, msg:
            self.mark_dead("connect: %s" % msg)
            return None
        except socket.error, msg:
            if isinstance(msg, tuple): 
                msg = msg[1]
            self.mark_dead("connect: %s" % msg[1])
            return None
        self.socket = s
        self.buffer = ''
        return s

    def send_cmd(self, cmd):
        yield self.socket.send('%s\r\n' % cmd)

    def send_cmds(self, cmds):
        """ cmds already has trailing \r\n's applied """
        yield self.socket.send(cmds)

    def readline(self):
        data = yield self.socket.read_until('\r\n')
        yield data[:-2]
#        recv = self.socket.recv
#        while True:
#            index = buf.find('\r\n')
#            if index >= 0:
#                break
#            data = recv(4096)
#            if not data:
#                self.mark_dead('Connection closed while reading from %s'
#                        % repr(self))
#                break
#            buf += data
#        if index >= 0:
#            self.buffer = buf[index+2:]
#            buf = buf[:index]
#        else:
#            self.buffer = ''
#        return buf

    def expect(self, text):
        line = yield self.readline()
        if line != text:
            self.debuglog("while expecting '%s', got unexpected response '%s'" % (text, line))
        yield line

    def recv(self, rlen):
        data = yield self.socket.read_bytes(rlen)
        yield data
#        self_socket_recv = self.socket.recv
#        buf = self.buffer
#        while len(buf) < rlen:
#            foo = self_socket_recv(4096)
#            buf += foo
#            if len(foo) == 0:
#                raise _Error, ( 'Read %d bytes, expecting %d, '
#                        'read returned 0 length bytes' % ( len(buf), rlen ))
#        self.buffer = buf[rlen:]
#        return buf[:rlen]