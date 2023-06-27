"""see class tattleRequestHandler"""

import datetime
import os
import sqlite3
import subprocess
import threading
import time
import traceback
from datetime import timedelta
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
    /archive/
      archive all but last 100 logs for each process, vacuum DB
    /register/<process>/<seconds>/description text
      register a process with tag <process> which should report ever <seconds> seconds
      repeating ok, just changes interval and description
    /log/<process>/msg. text
    /log/<process>/status/[OK|FAIL|ENABLE|DISABLE]/msg. text
    /log/<process>/status/DEFER/<seconds>
    """

    statuses = "OK", "FAIL", "DISABLE", "ENABLE", "DEFER", "DEFUNCT"
    status_level = {
        "OK": 0,
        "FAIL": 1,
        "DISABLE": 0,
        "ENABLEL": 0,
        "DEFER": 0,
        "DEFUNCT": 0,
        "HARD": 2,
    }
    levels = "clr", "mix", "bad"  # favicon path fragment by error severity

    def do_GET(self):
        self.query = None
        if "?" in self.path:
            self.path, self.query = self.path.split("?", 1)

        path = unquote(self.path.strip("/ "))
        self.args = path.split("/")

        dispatch = {
            "": self.show_status,
            "all": self.show_all,
            "archive": self.archive,
            "quit": self.quit,
            "test": self.init,
            "init": self.init,
            "register": self.register,
            "log": self.log,
            "show": self.show,
            "update": self.update,
            "report": self.reports,
            "favicon.ico": self.favicon,
        }
        paths_no_template = ["report", "favicon.ico"]
        use_template = self.args[0] not in paths_no_template

        if self.args[0] != "log" or self.query:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            if "Host" in self.headers:
                self.send_header("Refresh", "70; url=//%s" % self.headers["Host"])
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
            self.out(self.template["ftr"].format(time=time.asctime()))

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

    def archive(self):
        keep = 100

        self.out(self.entry("DB file %s..." % self.dbfile))
        self.out(self.entry("...exists: %s" % os.path.isfile(self.dbfile)))
        self.out(self.entry("Got connection ok..."))
        con = sqlite3.connect(self.dbfile)
        self.out(self.entry(bool(con)))
        cur = con.cursor()
        for proc in [i[0] for i in cur.execute("SELECT process FROM process")]:
            last = list(
                cur.execute(
                    "select timestamp from log where process = ? "
                    "order by timestamp desc limit ?",
                    [proc, keep],
                )
            )
            if len(last) == keep:
                mintime = last[-1][0]
                self.out(self.entry("%s %s" % (proc, mintime)))
                cur.execute(
                    "insert into old_data select * from log where process = "
                    "? and timestamp < ?",
                    [proc, mintime],
                )
                cur.execute(
                    "delete from log where process = ? and timestamp < ?",
                    [proc, mintime],
                )
        con.commit()
        self.out(self.entry("Vacuuming"))
        con.execute("vacuum")
        self.out(self.entry("Vacuuming done"))

        return "logged"

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
        table = "defer" if status == "DEFER" else "log"
        cur.execute(
            f"""insert into {table} (process, timestamp, status, message, ip)
            values (?,?,?,?,?)""",
            [tag, timestamp, status, message, self.client_address[0]],
        )
        con.commit()

    def out(self, s):
        if self.args[0] != "log" or self.query:
            self.wfile.write(s.encode("utf8") if isinstance(s, str) else s)

    def quit(self):
        self.out(self.entry("TERMINATING"))

        # wait 1.0 seconds for the request to finish before ending
        threading.Timer(1.0, lambda: self.server.shutdown()).start()

    _hms = {"d": 3600 * 24, "h": 3600, "m": 60, "s": 1}

    @classmethod
    def hms_to_s(cls, hms: str) -> float:
        """Convert 0d1h2m3.5s to 3723.5"""
        total = 0
        part = ""
        hms = list(hms)
        while hms:
            char = hms.pop(0)
            if char in cls._hms:
                total += float(part) * cls._hms[char]
                part = ""
            else:
                part += char
        if part:
            total += float(part)
        return total

    def register(self):
        if self.query:
            dat = parse_qs(self.query)
            tag, dummy = dat["proctype"][0].split("::")
            interval, description = dat["msg"][0].split("/", 1)

        else:
            cmd, tag, interval = self.args[:3]
            description = "/".join(self.args[3:])

        interval = self.hms_to_s(interval)

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
            description = "*unregistered process, assuming 5m interval*"
            interval = 300
        else:
            description, interval = description
            interval = float(interval)

        if not interval:
            interval = 300

        interval_td = datetime.timedelta(0, interval)
        self.out(
            "<h1>{process}: {intfmt} : {description}</h1>".format(
                process=tag, intfmt=self.td2str(interval_td), description=description
            )
        )

        cur.execute(
            """select * from log where process=? order by timestamp desc limit 20""",
            [tag],
        )
        logs = list(reversed(cur.fetchall()))

        for process, timestamp, status, message, ip in logs:
            timestamp = timestamp.split(".")[0]  # drop fractional seconds, for now
            timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

            if status == "FAIL":
                status = "HARD"

            if status in ("DISABLE", "ENABLE"):
                message = "%s: %s" % (status, message)

            self.out(self.entry(message, class_=status, ts=timestamp))

        self.out("<p/>")
        for status in "OK", "FAIL":
            if logs[-1][2] != status:
                cur.execute(
                    "select * from log where process=? and status=? "
                    "order by timestamp desc limit 20",
                    [tag, status],
                )
                log = cur.fetchall()
                if log:
                    process, timestamp, status_, message, ip = log[0]
                    self.out(
                        f"<div>Last {status}</div>"
                        + self.entry(message, class_=status_, ts=timestamp)
                    )
                else:
                    self.out(f"(no earlier {status} entries)")

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
                value="%s/%s" % (self.td2str(interval, exact=True), description),
            )
        )

    def show_help(self):
        self.out(self.template["help"].format(path=self.path))

    def show_all(self):
        self.show_status(show_all=True)

    def td2str(self, sep, exact=False):
        if not isinstance(sep, timedelta):
            sep = timedelta(seconds=float(sep))
        total = sep.total_seconds()
        sep = total
        rep = []
        for key, amount in self._hms.items():
            step = sep if key == "s" else sep // amount
            if step:
                rep.append(f"{step:02.0f}{key}")
                sep -= step * amount
                if not exact and sep / total < 0.1:
                    break
        return "".join(rep).lstrip("0")

    def delete_defers(self, con, cur):
        """Delete DEFER status if expired.  If *any* DEFER has expired, delete *all*
        DEFERs for that process, so you can DEFER a lower number later."""
        cur.execute(
            "select process, timestamp, min(cast(message as real)) as ttl "
            "from defer where status = 'DEFER'"
        )
        for process, timestamp, ttl in cur:
            if not timestamp:
                continue  # None, None, None possible
            timestamp = timestamp.split(".")[0]  # drop fractional seconds
            timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            elapsed = (datetime.datetime.now() - timestamp).total_seconds() / 3600
            if elapsed > ttl:
                con.execute(
                    "delete from defer where status = 'DEFER' and process = ?",
                    [process],
                )
                con.commit()

    def get_status(self, show_all=False):
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()

        self.delete_defers(con, cur)
        cur.execute("select distinct process from defer")
        defered = [i[0] for i in cur]

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
            if process in defered:
                status = "DEFER"

            assumed_interval = False
            if not interval:
                interval = 24.0
                assumed_interval = True

            if not description:
                description = "*unregistered process, assuming 24h interval*"

            details = ""

            interval_txt = self.td2str(interval)

            if last != 0:
                last = last.split(".")[0]  # drop fractional seconds, for now

                last_date = datetime.datetime.strptime(last, "%Y-%m-%d %H:%M:%S")

                now = datetime.datetime.now()

                interval_td = datetime.timedelta(0, interval)

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
            yield {
                "part": dict(
                    log_process=log_process,
                    details=details,
                    out_status=out_status,
                    timestamp=timestamp,
                    message=message,
                    spare=spare,
                )
            }

    def show_status(self, show_all=False):
        for status in self.get_status(show_all=show_all):
            self.out(
                "<div class='ent'>"
                "<span class='tag'>{log_process} <span title='{details}' "
                "class='ts {out_status}'>{timestamp} </span> </span>"
                " <span class='msg'> {message} <span class='time'>{spare}</span></span>"
                "</div>".format_map(status["part"])
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
    schema["old_data"] = schema["log"]
    schema["defer"] = schema["log"]

    def update(self):
        """Make a system call to run `tattle_update` in the background.

        tattle_update needs to be executable and on the path.
        """
        subprocess.Popen(
            "tattle_update &",
            shell=True,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
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
            self.out(self.template["ftr"].format(time=time.asctime()))

    def favicon(self):
        """Based on current status"""
        level = 0
        for status in self.get_status():
            level = max(level, self.status_level[status["part"]["out_status"]])
        path = Path(__file__).with_name("favicon_" + self.levels[level] + ".ico")
        self.out(path.read_bytes())

    colors = {
        "BACKGROUND": "white",
        "FOREGROUND": "black",
        "FAIL": "pink",
        "HARD": "red",
        "OK": "#afa",
        "DISABLE": "#ccc",
        "ENABLE": "cyan",
    }
    colors = {  # not finished
        "BACKGROUND": "black",
        "FOREGROUND": "#ccc",
        "FAIL": "#6c71c4",
        "HARD": "#d33682",
        "OK": "#859900",
        "DISABLE": "#2aa198",
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
            a {{ text-decoration: none; color: {FOREGROUND}; }}
            a:active {{ text-decoration: none; color: {FOREGROUND}; }}
            a:visited {{ text-decoration: none; color: {FOREGROUND}; }}
            a:hover {{ text-decoration: underline; color: red }}
            .right {{ text-align: right }}
            .time {{ clear: left; }}
            hr {{ border-style: solid; border-color: grey; border-width: 2px 0 0 0 ; }}
            </style>
            <title>Tattle</title>
            </head><body><div>
            <a href="/">Home</a>
            <a href="/all">Show disabled</a>
            <a href="/quit">Re-start</a>
            <a href="/update">Get updates</a>
            <a href="/report">Reports</a>
            </div><hr/>""".format(
            **colors
        ),
        "ftr": """<div class='time'>{time}</div></body></html>""",
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


def run(server_class=ThreadedServer, handler_class=tattleRequestHandler):
    server_address = ("0.0.0.0", 8111)
    httpd = server_class(server_address, handler_class)

    httpd.serve_forever()


if __name__ == "__main__":
    run()
