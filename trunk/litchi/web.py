#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""simple web application
"""

import re
import httplib

from litchi.http import HTTPReponse


class Application(object):
    
    def __init__(self, routes, settings={}):
        """
        @param routes: [[url_regex, handler], ...]
        """
        self.routes = [(re.compile(url_regex).match, handler) for url_regex, handler in routes]
        self.settings = settings
        if 'page_not_found' in settings:
            self.page_not_found = settings['page_not_found']
        else:
            self.page_not_found = lambda x: (yield HTTPNotFound())
        
    def __call__(self, request):
        handler, kwargs = self.get_handler(request)
        response = yield handler(request, **kwargs)
        yield response
        
    def get_handler(self, request):
        for match, handler in self.routes:
            m = match(request.path)
            if m:
                kwargs = m.groupdict()
                kwargs = dict((k, v) for k, v in kwargs.iteritems() if v is not None)
                return handler, kwargs
        return self.page_not_found, {}


class HTTPRedirect(HTTPReponse):
    
    def __init__(self, redirect_url):
        super(HTTPRedirect, self).__init__(headers={'Location': redirect_url}, status=httplib.FOUND)

class HTTPPermanentRedirect(HTTPRedirect):
    
    def __init__(self, *args, **kwargs):
        super(HTTPPermanentRedirect, self).__init__(*args, **kwargs)
        self.status = httplib.MOVED_PERMANENTLY

class HTTPNotFound(HTTPReponse):
    
    def __init__(self, *args, **kwargs):
        super(HTTPNotFound, self).__init__(*args, **kwargs)
        self.status = httplib.NOT_FOUND
        if not self.body:
            self.body = '<html><title>404</title><body>Page not found</body></html>'

class HTTPForbidden(HTTPReponse):
    
    def __init__(self, *args, **kwargs):
        super(HTTPForbidden, self).__init__(*args, **kwargs)
        self.status = httplib.FORBIDDEN

class HTTPServerError(HTTPReponse):
    
    def __init__(self, *args, **kwargs):
        super(HTTPServerError, self).__init__(*args, **kwargs)
        self.status = httplib.INTERNAL_SERVER_ERROR