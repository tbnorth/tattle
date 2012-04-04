"""see class tattleRequestHandler"""

import BaseHTTPServer
import datetime
import sqlite3
import os
import sys
import threading
import urllib
import urlparse
import traceback
import xml.sax.saxutils

q = xml.sax.saxutils.quoteattr
class tattleRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    tattle.py, dependency free simple batch monitoring system.

    /
      show status of all processes
    /quit/
      kill the server
    /init/
      create database if needed, with feedback
    /test/
      self test (same as init)
    /register/<process>/<hours>/description text
      register a process with tag <process> which should report ever <hours> hours
      repeating ok, just changes interval and description
    /log/<process>/msg. text
    /log/<process>/status/[OK|FAIL|ENABLE|DISABLE]/msg. text
    """
    def do_GET(self):

        self.args = ['']

        self.query = None
        if '?' in self.path:
            self.path, self.query = self.path.split('?', 1)

        self.args = urllib.unquote(self.path.strip('/ ')).split('/')

        dispatch = {
            '': self.show_status,
            'all': self.show_all,
            'quit': self.quit,
            'test': self.init,
            'init': self.init,
            'register': self.register,
            'log': self.log,
            'show': self.show,
        }


        if self.args[0] != 'log':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

        # self.out does nothing when self.args[0] == 'log'

        self.out(self.template['hdr'])
        try:
            if self.args and self.args[0] in dispatch:
                dispatch[self.args[0]]()
            else:
                self.show_help()
        except Exception:
            self.out("<pre>%s</pre>" % traceback.format_exc())
            raise
        self.out(self.template['ftr'])

        if self.args[0] == 'log':
            self.send_response(303)
            self.send_header('Location', '/')
            self.end_headers()
    def entry(self, s, class_='', ts=None, prefix=''):
        if class_.strip():
            class_ = ' '+class_.strip()
        if not ts:
            ts = datetime.datetime.now()

        if isinstance(ts, datetime.datetime):
            ts = ts.strftime("%d %H:%M:%S")

        return "<div>%s<span class='ts%s'>%s</span> %s</div>" % (
            prefix, class_, ts, s)

    def init(self):

        logs = []

        logs.append(self.entry("DB file %s..."%self.dbfile))
        logs.append(self.entry("...exits: %s"%os.path.isfile(self.dbfile)))
        logs.append(self.entry("Got connection ok..."))
        con = sqlite3.connect(self.dbfile)
        logs.append(self.entry(bool(con)))
        cur = con.cursor()
        cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        tables = [i[0] for i in cur.fetchall()]
        for table in self.schema:
            if table not in tables:
                logs.append(self.entry("Table '%s' doesn't exist, creating."%table))
                cur.execute('create table %s (%s)' % (table,
                    ','.join(["%s %s"%(i[0],i[1]) for i in self.schema[table]])))
            else:
                logs.append(self.entry("Table '%s' found ok"%table))
                cur.execute("PRAGMA table_info(%s)"%table)
                fields = [i[1] for i in cur.fetchall()]
                for field,type_ in self.schema[table]:
                    if field not in fields:

                        logs.append(
                            self.entry("Field '%s' doesn't exist, creating."%field))
                        cur.execute('alter table %s add %s %s' % (table,
                            field, type_))
                        con.commit()
                    else:
                        logs.append(self.entry("Field '%s' found ok"%field))

        self.out('\n'.join(logs))

        return 'logged'

    def log(self):

        args = self.args[:]
        args.pop(0)  # discard command name

        if self.query:
            dat = urlparse.parse_qs(self.query)
            tag,status = dat["proctype"][0].split("::")
            if '/STATUS/' in status:
                status = status.replace('/STATUS/', '')
            else:
                status = 'INFO'
            message = dat["msg"][0]
        else:
            tag = args.pop(0)
            if args and args[0] == 'status':
                args.pop(0)  # discard 'status'
                status = args.pop(0)
            else:
                status = 'INFO'
            if args:
                message = '/'.join(args)
            else:
                message = '*no msg.*'

        self.out(self.entry("'%s' says %s:%s" % (tag, status, message)))

        timestamp = datetime.datetime.now()

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        cur.execute("""insert into log (process, timestamp, status, message)
            values (?,?,?,?)""", [tag, timestamp, status, message])
        con.commit()
    def out(self, s):

        if self.args[0] != 'log':
            self.wfile.write(s)

    def quit(self):

        self.out(self.entry("TERMINATING"))

        # wait 1.0 seconds for the request to finish before ending
        threading.Timer(1.0, lambda:self.server.shutdown()).start()

    def register(self):

        if self.query:

            dat = urlparse.parse_qs(self.query)
            tag,dummy = dat["proctype"][0].split("::")
            interval,description = dat["msg"][0].split("/",1)

        else:

            cmd, tag, interval = self.args[:3]
            description = '/'.join(self.args[3:])

        interval = float(interval)

        self.out(self.entry("""Add/update '%s', "%s", interval=%f""" %
            (tag, description, interval)))

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()

        cur.execute('select * from process where process=?', [tag])
        process = cur.fetchall()
        if process:
            cur.execute("""update process set interval=?, description=?
                where process=?""", [interval, description, tag])
        else:
            cur.execute("""insert into process (process, description, interval)
                values (?,?,?)""", [tag, description, interval])
        con.commit()
    def setup(self):

        BaseHTTPServer.BaseHTTPRequestHandler.setup(self)

        self.dbfile = os.path.join(
            os.path.dirname(sys.argv[0]),
            os.path.splitext(os.path.basename(sys.argv[0]))[0]+'.sqlite')

    def show(self):

        args = self.args[:]
        args.pop(0)  # discard command name
        tag = args.pop(0)

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        cur.execute("""select description, interval from process where process=?""", [tag])
        description = cur.fetchone()
        if not description:
            description, interval = "*unregistered process, assuming 24h interval*", 24.
        else:
            description, interval = description
            interval = float(interval)

        if not interval:
            interval = 24.0

        interval_td = datetime.timedelta(0,3600.*interval)
        self.out('<h1>{process}: {intfmt} : {description}</h1>'.format(
            process=tag, intfmt=self.td2str(interval_td), description=description))

        cur.execute("""select * from log where process=? order by timestamp desc limit 20""", [tag])
        logs = reversed(cur.fetchall())

        for process, timestamp, status, message in logs:

            timestamp = timestamp.split('.')[0]  # drop fractional seconds, for now
            timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

            if status == 'FAIL':
                status = 'HARD'

            if status in ('DISABLE', 'ENABLE'):
                message = "%s: %s" % (status, message)
                
            self.out(self.entry(message, class_=status, ts=timestamp))

        for i in '', '/STATUS/FAIL', '/STATUS/OK':
            uptype = i
            type_=i.replace('STATUS', 'status')
            self.out(self.template['manual'].format(
                action='log', process=tag, type=type_, uptype=uptype, value=''))

        self.out(self.template['manual'].format(
            action='register', process=tag, type='', uptype='',
            value="%s/%s"%(interval,description)))
    def show_help(self):
        self.out(self.template['help'].format(path=self.path))
    def show_all(self):
        self.show_status(show_all=True)
    def show_status(self, show_all=False):

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()

        cur.execute("""create temporary table last_msg as
            select process, max(timestamp) as last from log
            group by process""")

        cur.execute("""
            select process, 0, 'NEW', process.*, 'NEW' from
            process left join log using (process) where log.process is null
              and (description is null or description not like 'DEFUNCT:%')

            union

            select last_msg.process, last, message, process.*, status from
            last_msg 
            join log on (last_msg.process = log.process and log.timestamp = last)
            left join process on (last_msg.process = process.process)

            where (description is null or description not like 'DEFUNCT:%')

            order by last
            """)

        for log_process, last, message, process, interval, description, status in cur.fetchall():
            
            if status == 'DISABLE' and not show_all:
                continue

            if not interval:
                interval = 24.0

            if not description:
                description = '*unregistered process, assuming 24h interval*'

            if last != 0:

                last = last.split('.')[0]  # drop fractional seconds, for now

                last_date = datetime.datetime.strptime(last, '%Y-%m-%d %H:%M:%S')

                now = datetime.datetime.now()

                interval_td = datetime.timedelta(0,3600.*interval)

                due = last_date + interval_td

                out_status = status
                if now > due or status not in ('OK', 'DISABLE', 'ENABLE'):
                    out_status = 'FAIL'
                if status == 'FAIL':
                    out_status = 'HARD'

                if now > due:
                    sep = now-due
                else:
                    sep = due-now

                spare = self.td2str(sep)

                log_process = "<a title=%s href=%s>%s</a> " % (
                    q(description), q('show/'+log_process), log_process)

                timestamp = last_date.strftime("%d&nbsp;%H:%M:%S")

            else:  # last == 0

                sep = 3600.*interval
                spare = "interval=%dd%dh%dm%ds" % (
                    sep//(3600*24),
                    sep%(3600*24)//3600,
                    sep%3600//60,
                    sep%60
                )
                timestamp = last_date = 'NEVER'
                out_status = 'FAIL'

                log_process = "<a title=%s href=%s>%s</a> " % (
                    q(description), q('show/'+log_process), log_process)

            self.out(
                "<div class='ent'>"
                "<span class='tag'>%s <span class='ts %s'>%s </span> </span>"
                " <span class='msg'> %s <span class='time'>%s</span></span>"
                "</div>" % (log_process, out_status, timestamp, message, spare))

    def td2str(self, sep):
        return "%dd%02d:%02d" % (
            sep.days,
            sep.seconds//3600,
            sep.seconds%3600//60,
            # sep.seconds%60
        )
    schema = {
        'process':
        [('process', 'text'), ('interval', 'float'), ('description', 'text'), 
         # ('active', 'boolean'), # ('test', 'test'), 
        ],
        'log':
        [('process', 'text'), ('timestamp', 'datetime'),
         ('status', 'text'), ('message', 'text'), ],
    }

    template = {
        'hdr':
        """<html><head><style>
        .FAIL { background: pink; }
        .HARD { background: red; }
        .OK { background: #afa; }
        .DISABLE { background: #ccc; }
        .ENABLE { background: cyan; }
        .ent { float: left; width: 49% }
        .tag { display: block; width: 40%; float: left; text-align:right; }
        .msg { display: block; float: left; width: 59%; padding-left: 1%;}
        .ts { color: blue; font-size: 75%; }
        .time { font-size: 75%; font-style: italic; }
        a { text-decoration: none; }
        a:hover { text-decoration: underline; color: red }
        .right { text-align: right }
        </style></head><body><div>
        <a href="/">Home</a>
        <a href="/all">Show disabled</a>
        <a href="/quit">Re-start</a>
        </div><hr/>""",

        'ftr':
        """</body></html>""",

        'help':
        """<pre>HELP</pre>
        <pre>{path}</pre>""",

        'manual':
        """<form method="get" action="/{action}">
        <div class='right'>/{action}/{process}{type}/<input name='msg' size='80'
          value='{value}'/>
        <input type='hidden' name='proctype' value='{process}::{uptype}'/></div>
        </form>"""
    }


def run(server_class=BaseHTTPServer.HTTPServer,
        handler_class=tattleRequestHandler):

    server_address = ('0.0.0.0', 8111)
    httpd = server_class(server_address, handler_class)

    httpd.serve_forever()



if __name__ == '__main__':
    run()
