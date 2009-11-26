#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""http server
"""

import socket
import fcntl
import errno
import urlparse
import time
import logging
import httplib
import Cookie
import datetime
import calendar
import email.utils

from litchi.socketwrap import Socket
from litchi.systemcall import NewTask


class HTTPServer(object):
    """HTTP server."""
    def __init__(self, handle_target, no_keep_alive=False, xheaders=False, ssl_options=None):
        self.handle_target = handle_target
        self.no_keep_alive = no_keep_alive
        self.xheaders = xheaders
        self.ssl_options = ssl_options
        self._socket = None

    def listen(self, port, address=""):
        assert not self._socket
        self._socket = Socket()
#         Make a file descriptor close-on-exec.
        flags = fcntl.fcntl(self._socket.fileno(), fcntl.F_GETFD)
        flags |= fcntl.FD_CLOEXEC
        fcntl.fcntl(self._socket.fileno(), fcntl.F_SETFD, flags)
        # reuse address
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
#         set non blocking
        self._socket.setblocking(0)
        
        self._socket.bind((address, port))
        self._socket.listen(65535)

    def start(self):
        while True:
            try:
                connection, address = yield self._socket.accept()
            except socket.error, e:
                if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN): # omit Operation would block and Try again error
                    return
                raise
#            print 'new', connection, address, connection.fileno()
            yield NewTask(HTTPConnection(connection, address, self.handle_target).handler(), 'httphandler')
#            if self.ssl_options is not None:
#                assert ssl, "Python 2.6+ and OpenSSL required for SSL"
#                connection = ssl.wrap_socket(
#                    connection, server_side=True, **self.ssl_options)
#            try:
#                stream = iostream.IOStream(connection, io_loop=self.io_loop)
#                HTTPConnection(stream, address, self.request_callback,
#                               self.no_keep_alive, self.xheaders)
#            except:
#                logging.error("Error in connection callback", exc_info=True)


class HTTPConnection(object):
    """Handles a connection to an HTTP client, executing HTTP requests.

    We parse HTTP headers and bodies, and execute the request callback
    until the HTTP conection is closed.
    """
    def __init__(self, stream, address, handle_target, no_keep_alive=False, xheaders=False):
        self.stream = stream
        self.address = address
        self.handle_target = handle_target
        self.no_keep_alive = no_keep_alive
        self.xheaders = xheaders
        self._request = None

    def _check_disconnect(self):
        if self.no_keep_alive:
            disconnect = True
        else:
            connection_header = self._request.headers.get("Connection")
            if self._request.supports_http_1_1():
                disconnect = connection_header == "close"
            elif ("Content-Length" in self._request.headers 
                    or self._request.method in ("HEAD", "GET")):
                disconnect = connection_header != "Keep-Alive"
            else:
                disconnect = True
        self._request = None
        return disconnect

    def handler(self):
        try:
            while True:
                header_datas = yield self.stream.read_until('\r\n\r\n')
                eol = header_datas.find("\r\n")
                start_line = header_datas[:eol]
                method, uri, version = start_line.split(" ")
                if not version.startswith("HTTP/"):
                    raise Exception("Malformed HTTP version in HTTP Request-Line")
                headers = HTTPHeaders.parse(header_datas[eol:])
                self._request = HTTPRequest(
                    connection=self, method=method, uri=uri, version=version,
                    headers=headers, remote_ip=self.address[0])
                
                content_length = headers.get("Content-Length")
                if content_length:
                    content_length = int(content_length)
                    body_datas = yield self.stream.read_bytes(content_length)
                    self._parse_request_body(body_datas)
                response = yield self.handle_target(self._request)
                if not isinstance(response, HTTPReponse): # get string
                    response = HTTPReponse(response, self._request)
                yield self.stream.send(response.format())
                if self._check_disconnect():
                    break
        finally:
            self.stream.close()
            
#            if content_length > self.stream.max_buffer_size:
#                raise Exception("Content-Length too long")
#            if headers.get("Expect") == "100-continue":
#                self.stream.write("HTTP/1.1 100 (Continue)\r\n\r\n")
            
#            self.stream.read_bytes(content_length, self._on_request_body)
#            return
#
#        self.request_callback(self._request)

    def _parse_request_body(self, data):
        self._request.body = data
        if self._request.method == "POST":
            content_type = self._request.headers.get("Content-Type", "")
            if content_type.startswith("application/x-www-form-urlencoded"):
                arguments = urlparse.parse_qs(self._request.body)
                for name, values in arguments.iteritems():
                    values = [v for v in values if v]
                    if values:
                        self._request.arguments.setdefault(name, []).extend(
                            values)
            elif content_type.startswith("multipart/form-data"):
                boundary = content_type[30:]
                if boundary: 
                    self._parse_mime_body(boundary, data)

    def _parse_mime_body(self, boundary, data):
        if data.endswith("\r\n"):
            footer_length = len(boundary) + 6
        else:
            footer_length = len(boundary) + 4
        parts = data[:-footer_length].split("--" + boundary + "\r\n")
        for part in parts:
            if not part: continue
            eoh = part.find("\r\n\r\n")
            if eoh == -1:
                logging.warning("multipart/form-data missing headers")
                continue
            headers = HTTPHeaders.parse(part[:eoh])
            name_header = headers.get("Content-Disposition", "")
            if not name_header.startswith("form-data;") or \
               not part.endswith("\r\n"):
                logging.warning("Invalid multipart/form-data")
                continue
            value = part[eoh + 4:-2]
            name_values = {}
            for name_part in name_header[10:].split(";"):
                name, name_value = name_part.strip().split("=", 1)
                name_values[name] = name_value.strip('"').decode("utf-8")
            if not name_values.get("name"):
                logging.warning("multipart/form-data value missing name")
                continue
            name = name_values["name"]
            if name_values.get("filename"):
                ctype = headers.get("Content-Type", "application/unknown")
                self._request.files.setdefault(name, []).append(dict(
                    filename=name_values["filename"], body=value,
                    content_type=ctype))
            else:
                self._request.arguments.setdefault(name, []).append(value)
                

class HTTPRequest(object):
    """A single HTTP request.

    GET/POST arguments are available in the arguments property, which
    maps arguments names to lists of values (to support multiple values
    for individual names). Names and values are both unicode always.

    File uploads are available in the files property, which maps file
    names to list of files. Each file is a dictionary of the form
    {"filename":..., "content_type":..., "body":...}. The content_type
    comes from the provided HTTP header and should not be trusted
    outright given that it can be easily forged.

    An HTTP request is attached to a single HTTP connection, which can
    be accessed through the "connection" attribute. Since connections
    are typically kept open in HTTP/1.1, multiple requests can be handled
    sequentially on a single connection.
    """
    def __init__(self, method, uri, version="HTTP/1.1", headers=None,
                 body='', remote_ip=None, protocol=None, host=None,
                 files=None, connection=None):
        self.method = method
        self.uri = uri
        self.version = version
        self.headers = headers or HTTPHeaders()
        self.body = body
        if connection and connection.xheaders:
            self.remote_ip = headers.get("X-Real-Ip", remote_ip)
            self.protocol = headers.get("X-Scheme", protocol) or "http"
        else:
            self.remote_ip = remote_ip
            self.protocol = protocol or "http"
        self.host = host or headers.get("Host") or "127.0.0.1"
        self.files = files or {}
        self.connection = connection
        self._start_time = time.time()
        self._finish_time = None

        scheme, netloc, path, query, fragment = urlparse.urlsplit(uri)
        self.path = path
        self.query = query
        arguments = urlparse.parse_qs(query)
        self.arguments = {}
        for name, values in arguments.iteritems():
            values = [v for v in values if v]
            if values: self.arguments[name] = values
        self.load_cookies_errors = False
    
    def get_argument(self, name, default=None, strip=True):
        """Returns the value of the argument with the given name.
        The returned value is always utf-8 encode string.
        """
        value = self.arguments.get(name, default)
        if value is not default:
            value = value[-1]
            if strip: 
                value = value.strip()
        return value
    
    def supports_http_1_1(self):
        """Returns True if this request supports HTTP/1.1 semantics"""
        return self.version == "HTTP/1.1"

    def full_url(self):
        """Reconstructs the full URL for this request."""
        return self.protocol + "://" + self.host + self.uri

    def request_time(self):
        """Returns the amount of time it took for this request to execute."""
        if self._finish_time is None:
            return time.time() - self._start_time
        else:
            return self._finish_time - self._start_time

    def __repr__(self):
        attrs = ("protocol", "host", "method", "uri", "version", "remote_ip",
                 "remote_ip", "body")
        args = ", ".join(["%s=%r" % (n, getattr(self, n)) for n in attrs])
        return "%s(%s, headers=%s)" % (
            self.__class__.__name__, args, dict(self.headers))
        
    @property
    def cookies(self):
        """A dictionary of Cookie.Morsel objects."""
        if not hasattr(self, "_cookies"):
            self._cookies = Cookie.BaseCookie()
            if "Cookie" in self.headers:
                try:
                    self._cookies.load(self.headers["Cookie"])
                except:
                    self.load_cookies_errors = True
        return self._cookies

    def get_cookie(self, name, default=None):
        """Gets the value of the cookie with the given name, else default."""
        if name in self.cookies:
            return self.cookies[name].value
        return default


class HTTPHeaders(dict):
    """A dictionary that maintains Http-Header-Case for all keys."""
    def __setitem__(self, name, value):
        dict.__setitem__(self, self._normalize_name(name), value)

    def __getitem__(self, name):
        return dict.__getitem__(self, self._normalize_name(name))

    def _normalize_name(self, name):
        return "-".join([w.capitalize() for w in name.split("-")])

    @classmethod
    def parse(cls, headers_string):
        headers = cls()
        for line in headers_string.splitlines():
            if line:
                name, value = line.split(": ", 1)
                headers[name] = value
        return headers

def _utf8(s):
    if isinstance(s, unicode):
        return s.encode("utf-8")
    assert isinstance(s, str)
    return s

class HTTPReponse(object):
    """HTTP Reponse"""
    default_content_type = 'text/html'
    default_charset = 'UTF-8'
    default_server = 'Litchi/0.1'
    
    def __init__(self, body, request, status=httplib.OK, headers=None, content_type=default_content_type):
        self.body = body
        self.status = status
        self.headers = HTTPHeaders()
        self.headers["Server"] = self.default_server
        if headers is not None:
            self.headers.update(headers)
        self.content_type = content_type
        if self.content_type.startswith('text/'):
            self.headers['Content-Type'] = '%s; charset=%s' % (self.content_type, self.default_charset)
        self.request = request
        
    def format(self):
        # protocol, status
        datas = ['%s %s %s' % (self.request.version, self.status, httplib.responses[self.status])]
        # headers
        if not self.request.supports_http_1_1():
            if self.request.headers.get('Connection') == 'Keep-Alive':
                self.headers['Connection'] = 'Keep-Alive' # keep alive if client have the header
        # body length header
        if 'Content-Length' not in self.headers:
            self.headers['Content-Length'] = len(self.body)
        datas.extend(['\r\n%s: %s' % (n, v) for n, v in self.headers.iteritems()])
        # cookies
        if self.request.load_cookies_errors: # client cookie raw data is error format.
            self.clear_all_cookies()
        for cookie_dict in getattr(self, "_new_cookies", []):
            for cookie in cookie_dict.values():
                datas.append("\r\nSet-Cookie: %s" % cookie.OutputString())
        datas.append('\r\n\r\n')
        # body
        datas.append(self.body)
        return ''.join(datas)

    def set_cookie(self, name, value, path="/", domain=None, expires=None, expires_days=None):
        """Sets the given cookie name/value with the given options."""
        name = _utf8(name)
        value = _utf8(value)
#        if re.search(r"[\x00-\x20]", name + value):
#            # Don't let us accidentally inject bad stuff
#            raise ValueError("Invalid cookie %r: %r" % (name, value))
        if not hasattr(self, "_new_cookies"):
            self._new_cookies = []
        new_cookie = Cookie.BaseCookie()
        self._new_cookies.append(new_cookie)
        new_cookie[name] = value
        if domain:
            new_cookie[name]["domain"] = domain
        if expires_days is not None and not expires:
            expires = datetime.datetime.utcnow() + datetime.timedelta(days=expires_days)
        if expires:
            timestamp = calendar.timegm(expires.utctimetuple())
            new_cookie[name]["expires"] = email.utils.formatdate(timestamp, localtime=False, usegmt=True)
        if path:
            new_cookie[name]["path"] = path

    def clear_cookie(self, name, path="/", domain=None):
        """Deletes the cookie with the given name."""
        expires = datetime.datetime.utcnow() - datetime.timedelta(days=365)
        self.set_cookie(name, value="", path=path, domain=domain, expires=expires)

    def clear_all_cookies(self):
        """Deletes all the cookies the user sent with this request."""
        for name in self.request.cookies.iterkeys():
            self.clear_cookie(name)