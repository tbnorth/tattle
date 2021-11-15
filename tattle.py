"""see class tattleRequestHandler"""

import datetime
import os
import sqlite3
import subprocess
import threading
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, unquote
from xml.sax.saxutils import quoteattr


class tattleRequestHandler(BaseHTTPRequestHandler):
    """
    tattle.py, dependency free simple status monitoring system.

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
    /log/<process>/status/DEFER/<hours>
    """

    statuses = "OK", "FAIL", "DISABLE", "ENABLE", "DEFER", "DEFUNCT"

    def do_GET(self):

        self.query = None
        if "?" in self.path:
            self.path, self.query = self.path.split("?", 1)

        path = unquote(self.path.strip("/ "))
        self.args = path.split("/")

        dispatch = {
            "": self.show_status,
            "all": self.show_all,
            "quit": self.quit,
            "test": self.init,
            "init": self.init,
            "register": self.register,
            "log": self.log,
            "show": self.show,
            "update": self.update,
            "report": self.reports,
        }
        paths_no_template = ["report"]
        use_template = self.args[0] not in paths_no_template

        if self.args[0] != "log" or self.query:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

        # self.out does nothing when self.args[0] == 'log'

        if use_template:
            self.out(self.template["hdr"])
        try:
            if self.args and self.args[0] in dispatch:
                dispatch[self.args[0]]()
            else:
                self.show_help()
        except Exception:
            self.out("<pre>%s</pre>" % traceback.format_exc())
            raise
        if use_template:
            self.out(self.template["ftr"])

        if self.args[0] == "log" and not self.query:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(f"{path} ACKNOWLEDGED\n".encode("utf8"))

    def entry(self, s, class_="", ts=None, prefix=""):
        if class_.strip():
            class_ = " " + class_.strip()
        if not ts:
            ts = datetime.datetime.now()

        if isinstance(ts, datetime.datetime):
            ts = ts.strftime("%d %H:%M:%S")

        return "<div>%s<span class='ts%s'>%s</span> %s</div>" % (prefix, class_, ts, s)

    def init(self):

        logs = []

        logs.append(self.entry("DB file %s..." % self.dbfile))
        logs.append(self.entry("...exists: %s" % os.path.isfile(self.dbfile)))
        logs.append(self.entry("Got connection ok..."))
        con = sqlite3.connect(self.dbfile)
        logs.append(self.entry(bool(con)))
        cur = con.cursor()
        cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        tables = [i[0] for i in cur.fetchall()]
        for table in self.schema:
            if table not in tables:
                logs.append(self.entry("Table '%s' doesn't exist, creating." % table))
                cur.execute(
                    "create table %s (%s)"
                    % (
                        table,
                        ",".join(["%s %s" % (i[0], i[1]) for i in self.schema[table]]),
                    )
                )
                for i in self.schema[table]:
                    if len(i) > 2 and i[2]:
                        cur.execute(
                            "create %s %s_%s_idx on %s (%s)"
                            % (
                                i[2],  # 'index' or 'unique index'
                                table,
                                i[0],
                                table,
                                i[0],
                            )
                        )
            else:
                logs.append(self.entry("Table '%s' found ok" % table))
                cur.execute("PRAGMA table_info(%s)" % table)
                fields = [i[1] for i in cur.fetchall()]
                for field, type_, index in [
                    (i + (None,))[:3] for i in self.schema[table]
                ]:
                    if field not in fields:

                        logs.append(
                            self.entry("Field '%s' doesn't exist, creating." % field)
                        )
                        cur.execute("alter table %s add %s %s" % (table, field, type_))
                        if index:
                            cur.execute(
                                "create %s %s_%s_idx on %s (%s)"
                                % (
                                    index,  # 'index' or 'unique index'
                                    table,
                                    field,
                                    table,
                                    field,
                                )
                            )
                        con.commit()
                    else:
                        logs.append(self.entry("Field '%s' found ok" % field))

        self.out("\n".join(logs))

        return "logged"

    def log(self):

        args = self.args[:]
        args.pop(0)  # discard command name

        if self.query:
            dat = parse_qs(self.query)
            tag, status = dat["proctype"][0].split("::")
            if "/STATUS/" in status:
                status = status.replace("/STATUS/", "")
            else:
                status = "INFO"
            message = dat["msg"][0]
        else:
            tag = args.pop(0)
            if args and args[0] == "status":
                args.pop(0)  # discard 'status'
                status = args.pop(0)
            else:
                status = "INFO"
            if args:
                message = "/".join(args)
            else:
                message = "*no msg.*"

        self.out(self.entry("'%s' says %s:%s" % (tag, status, message)))

        timestamp = datetime.datetime.now()

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        cur.execute(
            """insert into log (process, timestamp, status, message, ip)
            values (?,?,?,?,?)""",
            [tag, timestamp, status, message, self.client_address[0]],
        )
        con.commit()

    def out(self, s):

        if self.args[0] != "log" or self.query:
            self.wfile.write(s.encode("utf8"))

    def quit(self):

        self.out(self.entry("TERMINATING"))

        # wait 1.0 seconds for the request to finish before ending
        threading.Timer(1.0, lambda: self.server.shutdown()).start()

    def register(self):

        if self.query:

            dat = parse_qs(self.query)
            tag, dummy = dat["proctype"][0].split("::")
            interval, description = dat["msg"][0].split("/", 1)

        else:

            cmd, tag, interval = self.args[:3]
            description = "/".join(self.args[3:])

        interval = float(interval)

        self.out(
            self.entry(
                """Add/update '%s', "%s", interval=%f""" % (tag, description, interval)
            )
        )

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()

        cur.execute("select * from process where process=?", [tag])
        process = cur.fetchall()
        if process:
            cur.execute(
                """update process set interval=?, description=?
                where process=?""",
                [interval, description, tag],
            )
        else:
            cur.execute(
                """insert into process (process, description, interval)
                values (?,?,?)""",
                [tag, description, interval],
            )
        con.commit()

    def setup(self):

        super().setup()

        self.dbfile = "tattle.sqlite"

    def show(self):

        args = self.args[:]
        args.pop(0)  # discard command name
        tag = args.pop(0)

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        cur.execute(
            """select description, interval from process where process=?""", [tag]
        )
        description = cur.fetchone()
        if not description:
            description, interval = (
                "*unregistered process, assuming 24h interval*",
                24.0,
            )
        else:
            description, interval = description
            interval = float(interval)

        if not interval:
            interval = 24.0

        interval_td = datetime.timedelta(0, 3600.0 * interval)
        self.out(
            "<h1>{process}: {intfmt} : {description}</h1>".format(
                process=tag, intfmt=self.td2str(interval_td), description=description
            )
        )

        cur.execute(
            """select * from log where process=? order by timestamp desc limit 20""",
            [tag],
        )
        logs = reversed(cur.fetchall())

        for process, timestamp, status, message, ip in logs:

            timestamp = timestamp.split(".")[0]  # drop fractional seconds, for now
            timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

            if status == "FAIL":
                status = "HARD"

            if status in ("DISABLE", "ENABLE"):
                message = "%s: %s" % (status, message)

            self.out(self.entry(message, class_=status, ts=timestamp))

        for i in "", "/STATUS/FAIL", "/STATUS/OK", "/STATUS/DEFER":
            uptype = i
            type_ = i.replace("STATUS", "status")
            self.out(
                self.template["manual"].format(
                    action="log", process=tag, type=type_, uptype=uptype, value=""
                )
            )

        self.out(
            self.template["manual"].format(
                action="register",
                process=tag,
                type="",
                uptype="",
                value="%s/%s" % (interval, description),
            )
        )

    def show_help(self):
        self.out(self.template["help"].format(path=self.path))

    def show_all(self):
        self.show_status(show_all=True)

    def td2str(self, sep):
        return "%dd%02d:%02d" % (
            sep.days,
            sep.seconds // 3600,
            sep.seconds % 3600 // 60,
            # sep.seconds%60
        )

    def delete_defers(self, con, cur):
        """Delete DEFER status if expired.  If *any* DEFER has expired, delete *all*
        DEFERs for that process, so you can DEFER a lower number later."""
        cur.execute(
            "select process, timestamp, min(cast(message as real)) as ttl "
            "from log where status = 'DEFER'"
        )
        for process, timestamp, ttl in cur:
            if not timestamp:
                continue  # None, None, None possible
            timestamp = timestamp.split(".")[0]  # drop fractional seconds
            timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            elapsed = (datetime.datetime.now() - timestamp).total_seconds() / 3600
            if elapsed > ttl:
                con.execute(
                    "delete from log where status = 'DEFER' and process = ?", [process]
                )
                con.commit()

    def show_status(self, show_all=False):

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()

        self.delete_defers(con, cur)

        # (?, ?, ?) for statuses
        in_clause = "(" + ",".join("?" * len(self.statuses)) + ")"
        cur.execute(
            f"""create temporary table last_msg as
            select process, max(timestamp) as last from log where status in {in_clause}
            group by process""",
            self.statuses,
        )

        cur.execute(
            """
            select process, 0, 'NEW', process.*, 'NEW', 'NEW' from
            process left join log using (process) where log.process is null
              and (description is null or description not like 'DEFUNCT:%')

            union

            select last_msg.process, last, message, process.*, status, ip from
            last_msg
            join log on (last_msg.process = log.process and log.timestamp = last)
            left join process on (last_msg.process = process.process)

            where (description is null or description not like 'DEFUNCT:%')

            order by last
            """
        )

        for (
            log_process,
            last,
            message,
            process,
            interval,
            description,
            status,
            ip,
        ) in cur.fetchall():

            if status == "DISABLE" and not show_all:
                continue

            assumed_interval = False
            if not interval:
                interval = 24.0
                assumed_interval = True

            if not description:
                description = "*unregistered process, assuming 24h interval*"

            details = ""

            sep = 3600.0 * interval
            interval_txt = "%dd%dh%dm" % (
                sep // (3600 * 24),
                sep % (3600 * 24) // 3600,
                sep % 3600 // 60,
            )

            if last != 0:

                last = last.split(".")[0]  # drop fractional seconds, for now

                last_date = datetime.datetime.strptime(last, "%Y-%m-%d %H:%M:%S")

                now = datetime.datetime.now()

                interval_td = datetime.timedelta(0, 3600.0 * interval)

                due = last_date + interval_td

                out_status = status
                if status != "DEFER" and (
                    now > due or status not in ("OK", "DISABLE", "ENABLE")
                ):
                    out_status = "HARD" if description.strip()[-1] == "*" else "FAIL"
                if status == "FAIL":
                    out_status = "HARD"

                if now > due:
                    sep = now - due
                    spare = "-" + self.td2str(sep)
                else:
                    sep = due - now
                    spare = "+" + self.td2str(sep)

                timestamp = last_date.strftime("%d&nbsp;%H:%M:%S")

                details = ", last %s, %s %s" % (
                    last_date.strftime("%b %d %Y %H:%M"),
                    "overdue" if now > due else "due",
                    due.strftime("%b %d %Y %H:%M"),
                )

            else:  # last == 0

                spare = "interval=" + interval_txt
                timestamp = last_date = "NEVER"
                out_status = "FAIL"

            details = "Every %s%s%s, %s" % (
                interval_txt,
                " (assumed)" if assumed_interval else "",
                details,
                ip,
            )

            log_process = "<a title=%s href=%s>%s</a> " % (
                quoteattr(description),
                quoteattr("show/" + log_process),
                log_process,
            )

            self.out(
                "<div class='ent'>"
                "<span class='tag'>%s <span title='%s'class='ts %s'>%s </span> </span>"
                " <span class='msg'> %s <span class='time'>%s</span></span>"
                "</div>" % (log_process, details, out_status, timestamp, message, spare)
            )

    schema = {
        "process": [
            ("process", "text", "unique index"),
            ("interval", "float"),
            ("description", "text"),
            # ('active', 'boolean'), # ('test', 'test'),
        ],
        "log": [
            ("process", "text", "index"),
            ("timestamp", "datetime", "index"),
            ("status", "text"),
            ("message", "text"),
            ("ip", "text"),
        ],
    }

    def update(self):
        """Make a system call to run `tattle_update` in the background.

        tattle_update needs to be executable and on the path.
        """
        subprocess.run("tattle_update &", shell=True)
        self.out("<p><code>tattle_update</code> called.</p>")

    def reports(self):
        """Show / serve reports"""
        if len(self.args) > 1:
            self.out((Path("reports") / self.args[1]).read_text())
        else:
            self.out(self.template["hdr"])
            for path in Path("reports").glob("*"):
                self.out(
                    "<div><a target='blank' "
                    f"href='/report/{path.name}'>{path.name}</a></div>"
                )
            self.out(self.template["ftr"])

    colors = {  # not finished
        "BACKGROUND": "black",
        "FOREGROUND": "gray",
        "FAIL": "pink",
        "HARD": "red",
        "OK": "#afa",
        "DISABLE": "#ccc",
        "ENABLE": "cyan",
    }
    colors = {
        "BACKGROUND": "white",
        "FOREGROUND": "black",
        "FAIL": "pink",
        "HARD": "red",
        "OK": "#afa",
        "DISABLE": "#ccc",
        "ENABLE": "cyan",
    }
    if 0:  # color-blind friendly version
        colors = {
            "FAIL": "yellow",
            "HARD": "orange",
            "OK": "#aaf",
            "DISABLE": "#ccc",
            "ENABLE": "#afa",
        }

    template = {
        "hdr": """<html><head><style>
            body {{ font-family: sans-serif; font-size: 90%;
                   background: {BACKGROUND}; color: {FOREGROUND}; }}
            .FAIL {{ background: {FAIL}; }}
            .HARD {{ background: {HARD}; }}
            .OK {{ background: {OK}; }}
            .DISABLE {{ background: {DISABLE}; }}
            .ENABLE {{ background: {ENABLE}; }}
            .ent {{ float: left; width: 49% }}
            .tag {{ display: block; width: 40%; float: left; text-align:right; }}
            .msg {{ display: block; float: left; width: 59%; padding-left: 1%;}}
            .ts {{ color: blue; font-size: 75%; }}
            .time {{ font-size: 75%; font-style: italic; }}
            a {{ text-decoration: none; }}
            a:hover {{ text-decoration: underline; color: red }}
            .right {{ text-align: right }}
            </style></head><body><div>
            <a href="/"><button>Home</button></a>
            <a href="/all"><button>Show disabled</button></a>
            <a href="/quit"><button>Re-start</button></a>
            <a href="/update"><button>Get updates</button></a>
            <a href="/report"><button>Reports</button></a>
            </div><hr/>""".format(
            **colors
        ),
        "ftr": """</body></html>""",
        "help": """<pre>HELP</pre>
            <pre>{path}</pre>""",
        "manual": """<form method="get" action="/{action}">
            <div class='right'>/{action}/{process}{type}/<input name='msg' size='80'
              value='{value}'/>
            <input type='hidden' name='proctype' value='{process}::{uptype}'/></div>
            </form>""",
    }


class ThreadedServer(ThreadingMixIn, HTTPServer):
    pass


def run(server_class=HTTPServer, handler_class=tattleRequestHandler):

    server_address = ("0.0.0.0", 8111)
    httpd = server_class(server_address, handler_class)

    httpd.serve_forever()


if __name__ == "__main__":
    run()
