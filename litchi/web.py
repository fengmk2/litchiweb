#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""simple web application
"""

import re

from litchi.http import HTTPReponse


class Application(object):
    
    def __init__(self, routes):
        """
        @param routes: [[url_regex, handler], ...]
        """
        self.routes = [(re.compile(url_regex).match, handler) for url_regex, handler in routes]
        
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
        yield HTTPReponse('Page not found.', request, 404)
        