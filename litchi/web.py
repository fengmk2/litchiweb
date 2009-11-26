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
        
    def __call__(self, request):
        handler = self.get_handler(request)
        response = yield handler(request)
        yield response
        
    def get_handler(self, request):
        for match, handler in self.routes:
            if match(request.path):
                return handler
        return self.page_not_found
            
    def page_not_found(self, request):
        yield HTTPReponse('Page not found.', 404)


class HTTPRedirect(HTTPReponse):
    
    def __init__(self, redirect_url):
        super(HTTPRedirect, self).__init__('', httplib.FOUND, {'Location': redirect_url})

class HttpPermanentRedirect(HTTPRedirect):
    
    def __init__(self, *args, **kwargs):
        super(HttpPermanentRedirect, self).__init__(*args, **kwargs)
        self.status = httplib.MOVED_PERMANENTLY

class HTTPNotFound(HTTPReponse):
    
    def __init__(self, *args, **kwargs):
        super(HTTPNotFound, self).__init__(*args, **kwargs)
        self.status = httplib.NOT_FOUND

class HTTPForbidden(HTTPReponse):
    
    def __init__(self, *args, **kwargs):
        super(HTTPForbidden, self).__init__(*args, **kwargs)
        self.status = httplib.FORBIDDEN

class HTTPServerError(HTTPReponse):
    
    def __init__(self, *args, **kwargs):
        super(HTTPServerError, self).__init__(*args, **kwargs)
        self.status = httplib.INTERNAL_SERVER_ERROR