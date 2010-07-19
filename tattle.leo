<?xml version="1.0" encoding="utf-8"?>
<?xml-stylesheet ekr_test?>
<leo_file>
<leo_header file_format="2" tnodes="0" max_tnode_index="0" clone_windows="0"/>
<globals body_outline_ratio="0.5" body_secondary_ratio="0.5">
	<global_window_position top="50" left="50" height="500" width="700"/>
	<global_log_window_position top="0" left="0" height="0" width="0"/>
</globals>
<preferences/>
<find_panel_settings/>
<vnodes>
<v t="tbrown.20100620094124.9540"><vh>@chapters</vh></v>
<v t="tbrown.20100620094124.9539" a="E"><vh>@auto tattle.py</vh></v>
<v t="tbrown.20100620094124.13686"><vh>pygeotag.py</vh></v>
<v t="tbrown.20100620094124.9543"><vh>rhttp.py</vh></v>
<v t="tbrown.20100621100519.9729"><vh>cp</vh></v>
</vnodes>
<tnodes>
<t tx="tbrown.20100620094124.13686"># coding=utf8
import cgi
import json
import urlparse
import BaseHTTPServer
import threading
import Queue
import time
import webbrowser
import os


class QueueTimeout(Queue.Queue):
    """from http://stackoverflow.com/questions/1564501/add-timeout-argument-to-pythons-queue-join

    by Lukáš Lalinský
    """
    class NotFinished(Exception):
        pass

    def join_with_timeout(self, timeout):
        self.all_tasks_done.acquire()
        try:
            endtime = time.time() + timeout
            while self.unfinished_tasks:
                remaining = endtime - time.time()
                if remaining &lt;= 0.0:
                    raise self.NotFinished
                self.all_tasks_done.wait(remaining)
        finally:
            self.all_tasks_done.release()

class PyGeoTag(object):
    def __init__(self, callback=None, synchronous=False):

        self.basedir = os.path.dirname(__file__)

        self.synchronous = synchronous
        if callback is not None:
            self.callback = callback

        if synchronous:
            self.callback = self._store

        self.server_thread = None
        self.running = False
        self.address = ''
        self.port = 8008
        self.request_queue = QueueTimeout()
        self.data = None

        self.syncWait = threading.Condition()

        self.server = self.init_server()
    # def stop_server(self):
    #     self.current_server_thread = -1
    #     time.sleep(self.timeout+1)  # wait for server to exit

    def start_server(self):

        if False and self.synchronous:
            pass
        else:
            self.running = True
            self.server_thread = threading.Thread(target=self._run_server)
            self.server_thread.start()
    def _run_server(self):

        while self.running:
            self.server.handle_request()
    def init_server(self):

        class _handler(GeoTagRequestHandler):
            owner = self

        server_class = BaseHTTPServer.HTTPServer
        handler_class = _handler
        server_address = (self.address, self.port)
        httpd = server_class(server_address, handler_class)
        httpd.timeout = 2

        return httpd
    def stop_server(self):

        # make an attempt to empty the queue
        for i in range(self.request_queue.qsize()):
            try:
                self.request_queue.get_nowait()
            except Queue.Empty:
                pass

        self.request_queue.put({'__msg_type':'shutdown'})
        time.sleep(2)  # wait for the msg to be picked up
        self.running = False
    def open_server_page(self):
        webbrowser.open_new("http://%s:%d/" %
            (self.address or "127.0.0.1", self.port))
    def callback(self, data):
        print data
    def _store(self, data):
        self.data = data
    def show_position(self, data={}):

        print 'SHOWING',data

        data["__msg_type"] = "show_position"

        self.request_queue.put(data)
    def request_position(self, data={}):

        print 'REQUESTING',data

        data["__msg_type"] = "request_position"

        self.request_queue.put(data)
    def get_position(self, data={}):

        if not self.synchronous:
            self.running = False
            raise Exception("Can't call get_position in asynchronous mode")
        if not self.running:
            raise Exception("Server is not running")

        if data:
            self.request_position(data)

        while True:
            try:
                self.request_queue.join_with_timeout(2)
            except QueueTimeout.NotFinished:
                if self.running: 
                    continue
            break

        return self.data


class GeoTagRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    staticMap = {
        "": "template.html",
        "jquery.js": "jquery.js",
        "map.js": "map.js",
        "jquery.json-2.2.min.js": "jquery.json-2.2.min.js",
    }

    def log_message(*args):
        return

    def do_GET(self):

        if self.path.startswith("/QUIT"):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.out("TERMINATING SERVER")
            # threading.Timer(2,self.server.shutdown).start()
            self.owner.stop_server()
            return

        path = self.path.strip('/').split('/')

        if len(path) == 1 and path[0] in self.staticMap:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(file(
                os.path.join(self.owner.basedir,self.staticMap[path[0]])).read())
            return

        if self.path.startswith("/sendPos?"):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            data = urlparse.parse_qs(urlparse.urlparse(self.path).query)['data'][0]
            data = json.loads(data)
            was_requested = False
            if "__msg_type" in data:
                del data["__msg_type"]
                was_requested = True
            self.owner.callback(data)
            if was_requested:
                self.owner.request_queue.task_done()
            return

        if self.path.startswith("/getMessage"):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            #data = urlparse.parse_qs(urlparse.urlparse(self.path).query)['data'][0]
            #print repr(json.loads(data))
            try:
                data = self.owner.request_queue.get_nowait()
            except Queue.Empty:
                data = {}
            self.wfile.write(json.dumps(data))
            return

if __name__ == '__main__':
    pgt = PyGeoTag(synchronous=True)
    pgt.start_server()
    time.sleep(1)
    pgt.open_server_page()

    f = pgt.get_position
    f({"description": "Turtles"})
    f({"description": "Frogs", 'secret':7})
    f({"description": "Otters"})

    print "DONE"
    if pgt.synchronous:
        pgt.stop_server()

</t>
<t tx="tbrown.20100620094124.9540"></t>
<t tx="tbrown.20100620094124.9543">"""Serve remind calendar as HTML
"""

# rhttp.py $Id$
# Author: Terry Brown
# Created: Sun Sep 14 2008

import BaseHTTPServer
import subprocess
from lxml import etree
import datetime

class rRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)

        if 'css' in self.path:
            self.send_header('Content-type', 'text/css')
        elif 'png' in self.path:
            self.send_header('Content-type', 'image/png')
        else:
            self.send_header('Content-type', 'text/html')
        self.end_headers()

        R2H="/home/tbrown/Desktop/Package/remind-03.01.05/www/rem2html"
        CSS="/home/tbrown/Desktop/Package/remind-03.01.05/www/rem-default.css"
        PNG="/home/tbrown/Desktop/Package/remind-03.01.05/www"

        if 'png' in self.path:
            self.wfile.write(file(PNG+'/'+self.path.strip('/').split('/')[-1]).read())
        elif 'css' in self.path:
            self.wfile.write(file(CSS).read())
        else:
            try:
                arg = int(self.path.strip('/'))
            except ValueError:
                arg = 6

            back = 1
            if arg &lt; 0:
                back = -arg
                arg = back + 3

            now = datetime.date.today()
            then = now + datetime.timedelta(weeks = -4*back-1)
            start = then.strftime("%d %B %Y")
            target = now.strftime("%B %Y")

            cmd = ['remind', '-qp%d'%arg, '/home/tbrown/.reminders']
            cmd.extend(start.split())
            print cmd
            cmd = subprocess.Popen(cmd, stdout = subprocess.PIPE)
            dat, dummy = cmd.communicate()
            cmd = [R2H]
            cmd = subprocess.Popen(cmd, stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   bufsize = 1024)
            dat, dummy = cmd.communicate(dat)
            # dat = dat.replace('*TODAY*', 
            #                   "&lt;span style='background:pink'&gt;TODAY&lt;/span&gt;")
            parser = etree.HTMLParser()
            dom = etree.ElementTree(etree.fromstring(dat, parser))

            today = dom.xpath("//td[contains(., '*TODAY*')]")[0]
            today.set('style', "background: #b0ffb0")

            month = dom.xpath("//table[.//caption[contains(., '%s')]]"%target)[0]
            month.set('id', 'thismonth')

            dat = etree.tostring(dom.getroot())
            self.wfile.write(dat.replace('*TODAY*',''))
            # dom.write(self.wfile)

def run(server_class=BaseHTTPServer.HTTPServer,
        handler_class=rRequestHandler):
    server_address = ('', 8765)
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()

if __name__ == '__main__':
    run()
</t>
<t tx="tbrown.20100621100519.9729">scp tattle.py 131.212.122.222:~/tattle/trunk/
scp -P 10222 tattle.py 127.0.0.1:~/tattle/trunk/
</t>
</tnodes>
</leo_file>
