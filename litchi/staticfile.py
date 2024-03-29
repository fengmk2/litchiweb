#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""static file request handler
"""
import os

from litchi.web import HTTPNotFound

types = {
    'html': 'text/html',
    'htm': 'text/html',
    'shtml': 'text/html',
    'css': 'text/css',
    'xml': 'text/xml',
    'gif': 'image/gif',
    'jpeg': 'image/jpeg',
    'jpg': 'image/jpeg',
    'js': 'application/x-javascript',
    'atom': 'application/atom+xml',
    'rss': 'application/rss+xml',
    
    'mml': 'text/mathml',
    'txt': 'text/plain',
    'jad': 'text/vnd.sun.j2me.app-descriptor',
    'wml': 'text/vnd.wap.wml',
    'htc': 'text/x-component',
    
    'png': 'image/png',
    'tif': 'image/tiff',
    'tiff': 'image/tiff',
    'wbmp': 'image/vnd.wap.wbmp',
    'ico': 'image/x-icon',
    'jng': 'image/x-jng',
    'bmp': 'image/x-ms-bmp',
    'svg': 'image/svg+xml',
    
    'jar': 'application/java-archive',
    'war': 'application/java-archive',
    'ear': 'application/java-archive',
    'hqx': 'application/mac-binhex40',
    'doc': 'application/msword',
    'pdf': 'application/pdf',
    'ps': 'application/postscript',
    'eps': 'application/postscript',
    'ai': 'application/postscript',
    'rtf': 'application/rtf',
    'xls': 'application/vnd.ms-excel',
    'ppt': 'application/vnd.ms-powerpoint',
    
    'wmlc': 'application/vnd.wap.wmlc',
    'xhtml': 'application/vnd.wap.xhtml+xml',
    'kml': 'application/vnd.google-earth.kml+xml',
    'kmz': 'application/vnd.google-earth.kmz',
    'cco': 'application/x-cocoa',
    'jardiff': 'application/x-java-archive-diff',
    'jnlp': 'application/x-java-jnlp-file',
    'run': 'application/x-makeself',
    
    'pl': 'application/x-perl',
    'pm': 'application/x-perl',
    'prc': 'application/x-pilot',
    'pdb': 'application/x-pilot',
    'rar': 'application/x-rar-compressed',
    'rpm': 'application/x-redhat-package-manager',
    'sea': ' application/x-sea',
    'swf': 'application/x-shockwave-flash',
    'sit': 'application/x-stuffit',
    'tcl': 'application/x-tcl',
    'tk': 'application/x-tcl',
    'der': 'application/x-x509-ca-cert',
    'pem': 'application/x-x509-ca-cert',
    'crt': 'application/x-x509-ca-cert',
    'xpi': 'application/x-xpinstall',
    'zip': 'application/zip',
    
    'bin': 'application/octet-stream',
    'exe': 'application/octet-stream',
    'dll': 'application/octet-stream',
    'deb': 'application/octet-stream',
    'dmg': 'application/octet-stream',
    'eot': 'application/octet-stream',
    'iso': 'application/octet-stream',
    'img': 'application/octet-stream',
    'msi': 'application/octet-stream',
    'msp': 'application/octet-stream',
    'msm': 'application/octet-stream',
    
    'mid': 'audio/midi',
    'midi': 'audio/midi',
    'kar': 'audio/midi',
    'mp3': 'audio/mpeg',
    'ra': 'audio/x-realaudio',
    
    '3gpp': 'video/3gpp',
    '3gp': 'video/3gpp',
    'mpeg': ' video/mpeg',
    'mpg': 'video/mpeg',
    'mov': 'video/quicktime',
    'flv': 'video/x-flv',
    'mng': 'video/x-mng',
    'asx': 'video/x-ms-asf',
    'asf': 'video/x-ms-asf',
    'wmv': 'video/x-ms-wmv',
    'avi': 'video/x-msvideo',
}


class StaticHandler(object):
    """Static file handler"""
    def __init__(self, root):
        self.root = root
        
    def __call__(self, request):
        path = request.path
        filepath = os.path.join(self.root, path[1:])
        if not os.path.exists(filepath):
            yield HTTPNotFound()
        ext = path.split('.')[-1].lower()
        with open(filepath, 'rb') as f:
            yield f.read(), types.get(ext, None)