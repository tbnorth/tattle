"""see class tattleRequestHandler"""
import datetime
import os
import random
import sqlite3
import subprocess
import threading
import time
import traceback
from base64 import b64encode
from collections import defaultdict, namedtuple
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from itertools import chain, zip_longest
from pathlib import Path
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, unquote
from xml.sax.saxutils import quoteattr

import png


def execute_retry(con, cur, query, args=None):
    """Retry query execution on OperationalError, with backoff."""
    args = [] if args is None else args
    for attempt in range(5):
        try:
            cur.execute(query, args)
            con.commit()
            break
        except sqlite3.OperationalError:
            time.sleep(3 * attempt + 5 * random.random())


class tattleRequestHandler(BaseHTTPRequestHandler):
    """tattle.py, dependency free simple status monitoring system.

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
    /history/<process>
      report state transitions over current and archival data
    """

    statuses = "OK", "FAIL", "DISABLE", "ENABLE", "DEFER", "DEFUNCT"
    status_level = {
        "OK": 0,
        "FAIL": 1,
        "DISABLE": 0,
        "ENABLE": 0,
        "DEFER": 0,
        "DEFUNCT": 0,
        "HARD": 2,
    }
    levels = "clr", "mix", "bad"  # favicon path fragment by error severity

    def do_GET(self):
        """Handle GET requests"""
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
            "history": self.history,
            "update": self.update,
            "report": self.reports,
            "favicon.ico": self.favicon,
        }
        paths_no_template = ["report", "favicon.ico"]
        use_template = self.args[0] not in paths_no_template

        if self.args[0] != "log" or self.query:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            delay = 1 if self.query else 70
            if "Host" in self.headers:
                self.send_header(
                    "Refresh", f"{delay}; url=//%s/" % self.headers["Host"]
                )
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

    def entry(self, s, class_="", ts=None, prefix="", full_time=False):
        """Display a single entry."""
        if class_.strip():
            class_ = " " + class_.strip()
        if not ts:
            ts = datetime.datetime.now()

        if isinstance(ts, datetime.datetime):
            fmt = "%b %d %Y %H:%M:%S" if full_time else "%d %H:%M:%S"
            ts = ts.strftime(fmt)

        return "<div>%s<span class='ts%s'>%s</span> %s</div>" % (prefix, class_, ts, s)

    def archive(self):
        """Move all but keep records for each process to old_data, vacuum DB."""
        keep = 1000

        self.out(self.entry("DB file %s..." % self.dbfile))
        self.out(self.entry("...exists: %s" % os.path.isfile(self.dbfile)))
        self.out(self.entry("Got connection ok..."))
        con = sqlite3.connect(self.dbfile)
        self.out(self.entry(bool(con)))
        cur = con.cursor()
        cur.execute("SELECT process FROM process")
        for proc in [i[0] for i in list(cur)]:
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
                execute_retry(
                    con,
                    cur,
                    "insert into old_data select * from log where process = "
                    "? and timestamp < ?",
                    [proc, mintime],
                )
                execute_retry(
                    con,
                    cur,
                    "delete from log where process = ? and timestamp < ?",
                    [proc, mintime],
                )
        con.commit()
        self.out(self.entry("Vacuuming"))
        con.execute("vacuum")
        self.out(self.entry("Vacuuming done"))

        return "logged"

    def init(self):
        """Create tables as needed."""
        logs = []

        logs.append(self.entry("DB file %s..." % self.dbfile))
        logs.append(self.entry("...exists: %s" % os.path.isfile(self.dbfile)))
        logs.append(self.entry("Got connection ok..."))
        con = sqlite3.connect(self.dbfile)
        logs.append(self.entry(bool(con)))
        cur = con.cursor()
        execute_retry(con, cur, """SELECT name FROM sqlite_master WHERE type='table'""")
        tables = [i[0] for i in cur.fetchall()]
        for table in self.schema:
            if table not in tables:
                logs.append(self.entry("Table '%s' doesn't exist, creating." % table))
                execute_retry(
                    con,
                    cur,
                    "create table %s (%s)"
                    % (
                        table,
                        ",".join(
                            ["%s %s" % (i.name, i.type) for i in self.schema[table]]
                        ),
                    ),
                )
                for i in self.schema[table]:
                    if i.index:
                        execute_retry(
                            con,
                            cur,
                            "create %s %s_%s_idx on %s (%s)"
                            % (
                                i.index,  # 'index' or 'unique index'
                                table,
                                i.name,
                                table,
                                i.name,
                            ),
                        )
            else:
                logs.append(self.entry("Table '%s' found ok" % table))
                execute_retry(con, cur, "PRAGMA table_info(%s)" % table)
                fields = [i[1] for i in cur.fetchall()]
                for field in self.schema[table]:
                    if field.name not in fields:
                        logs.append(
                            self.entry(
                                "Field '%s' doesn't exist, creating." % field.name
                            )
                        )
                        execute_retry(
                            con,
                            cur,
                            "alter table %s add %s %s"
                            % (table, field.name, field.type),
                        )
                        if field.index:
                            execute_retry(
                                con,
                                cur,
                                "create %s %s_%s_idx on %s (%s)"
                                % (
                                    field.index,  # 'index' or 'unique index'
                                    table,
                                    field.name,
                                    table,
                                    field.name,
                                ),
                            )
                        con.commit()
                    else:
                        logs.append(self.entry("Field '%s' found ok" % field.name))

        self.out("\n".join(logs))

        return "logged"

    def log(self):
        """Log a message."""
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
        self.show_status()

        timestamp = datetime.datetime.now()

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        table = "defer" if status == "DEFER" else "log"
        execute_retry(
            con,
            cur,
            f"""insert into {table} (process, timestamp, status, message, ip)
            values (?,?,?,?,?)""",
            [tag, timestamp, status, message, self.client_address[0]],
        )

    def out(self, s):
        """Write to HTML output."""
        if self.args[0] != "log" or self.query:
            self.wfile.write(s.encode("utf8") if isinstance(s, str) else s)

    def quit(self):
        """Quit the server - good for reloading code."""
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
        """Register a process."""
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

        execute_retry(con, cur, "select * from process where process=?", [tag])
        process = cur.fetchall()
        if process:
            execute_retry(
                con,
                cur,
                """update process set interval=?, description=?
                where process=?""",
                [interval, description, tag],
            )
        else:
            execute_retry(
                con,
                cur,
                """insert into process (process, description, interval)
                values (?,?,?)""",
                [tag, description, interval],
            )
        con.commit()

    def setup(self):
        """Set up the handler."""
        super().setup()

        self.dbfile = "tattle.sqlite"

    def show(self):
        """Show a single process log."""
        args = self.args[:]
        args.pop(0)  # discard command name
        tag = args.pop(0)

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        execute_retry(
            con,
            cur,
            """select description, interval from process where process=?""",
            [tag],
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

        execute_retry(
            con,
            cur,
            """select * from log where process=? order by timestamp desc limit 1000""",
            [tag],
        )
        logs = list(reversed(cur.fetchall()))

        # Find and report last good / bad status
        for status in "OK", "FAIL":
            if logs and logs[-1][2] != status:
                execute_retry(
                    con,
                    cur,
                    "select * from log where process=? and status=? "
                    "order by timestamp desc limit 20",
                    [tag, status],
                )
                log = cur.fetchall()
                if not log:
                    execute_retry(
                        con,
                        cur,
                        "select * from old_data where process=? and status=? "
                        "order by timestamp desc limit 20",
                        [tag, status],
                    )
                    log = cur.fetchall()
                self.out("<div>")
                if log:
                    process, timestamp, status_, message, ip = log[0]
                    self.out(
                        f"Last {status}"
                        + self.entry(message, class_=status_, ts=timestamp).replace(
                            "div>", "span>"
                        )
                    )
                else:
                    self.out(f"(no earlier {status} entries)")
                self.out(f" <a href='/history/{tag}'>history</a>")
                self.out("</div>")

        # Show history.
        self.out("<p/>")
        logs.reverse()
        self.out("<div class='left-side'>")
        for process, timestamp, status, message, ip in logs:
            timestamp = timestamp.split(".")[0]  # drop fractional seconds, for now
            timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            if status == "FAIL":
                status = "HARD"
            if status in ("DISABLE", "ENABLE"):
                message = "%s: %s" % (status, message)

            self.out(self.entry(message, class_=status, ts=timestamp, full_time=True))

        self.out("</div>")
        self.out("<div class='right-side'>")
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
        self.out("</div>")

    def history(self):
        """Show a single process long-term history."""
        # START FIXME: duplicate code from show()
        args = self.args[:]
        args.pop(0)  # discard command name
        tag = args.pop(0)

        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        execute_retry(
            con,
            cur,
            """select description, interval from process where process=?""",
            [tag],
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

        # END FIXME: duplicate code from show()

        total = defaultdict(float)

        self.out("<div class='left-side'>")

        for source in "log", "old_data":
            self.out("<div>")
            execute_retry(
                con,
                cur,
                f"""select * from {source} where process=? order by timestamp desc""",
                [tag],
            )
            logs = list(reversed(cur.fetchall()))

            # Show history.
            logs.reverse()
            last_state = None
            last_time = None
            for process, timestamp, status, message, ip in logs:
                if status != last_state:
                    if last_state:
                        total[last_state] += self._report_history(
                            last_state, last_time, timestamp
                        )
                    last_state = status
                    last_time = timestamp

            if last_time:
                total[status] += self._report_history(status, last_time, timestamp)
            self.out("</div>")

        self.out("</div>")
        gt = sum(total.values())
        for key, value in total.items():
            style = "HARD" if key == "FAIL" else key
            self.out(
                f"<div><span class='{style} st'>{key}</span>: "
                f"{value:.3f} days, {value/gt*100:.2f}%</div>"
            )

    def _report_history(self, status, last_time, timestamp):
        last_time = last_time.split(".")[0]  # drop fractional seconds, for now
        last_time = datetime.datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
        timestamp = timestamp.split(".")[0]  # drop fractional seconds, for now
        timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        duration = last_time - timestamp
        style = "HARD" if status == "FAIL" else status
        self.out(
            f"<div><span class='{style} st'>{status}</span> "
            f"<span class='du'>for {duration}</span> from {last_time} to {timestamp}</div>"
        )
        return duration.total_seconds() / (24 * 3600)

    def show_help(self):
        """Show help."""
        self.out(self.template["help"].format(path=self.path))

    def show_all(self):
        """Show all processes."""
        self.show_status(show_all=True)

    def td2str(self, sep, exact=False):
        """Convert a timedelta to a compact string."""
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
        DEFERs for that process, so you can DEFER a lower number later.
        """
        execute_retry(
            con,
            cur,
            "select process, timestamp, min(cast(message as real)) as ttl "
            "from defer where status = 'DEFER'",
        )
        for process, timestamp, ttl in cur:
            if not timestamp:
                continue  # None, None, None possible
            timestamp = timestamp.split(".")[0]  # drop fractional seconds
            timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            elapsed = (datetime.datetime.now() - timestamp).total_seconds() / 3600
            if elapsed > ttl:
                execute_retry(
                    con,
                    cur,
                    "delete from defer where status = 'DEFER' and process = ?",
                    [process],
                )
                con.commit()

    def get_status(self, show_all=False):
        """Get status of all processes."""
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()

        self.delete_defers(con, cur)
        execute_retry(con, cur, "select distinct process from defer")
        defered = [i[0] for i in cur]

        # (?, ?, ?) for statuses
        in_clause = "(" + ",".join("?" * len(self.statuses)) + ")"
        execute_retry(
            con,
            cur,
            f"""create temporary table last_msg as
            select process, max(timestamp) as last from log where status in {in_clause}
            group by process""",
            self.statuses,
        )

        execute_retry(
            con,
            cur,
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
            """,
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

            log_process_link = "<a title=%s href=%s>%s</a> " % (
                quoteattr(description),
                quoteattr("show/" + log_process),
                log_process,
            )
            yield {
                "part": dict(
                    log_process=log_process_link,
                    process=log_process,
                    description=description,
                    details=details,
                    out_status=out_status,
                    timestamp=timestamp,
                    message=message,
                    spare=spare,
                )
            }

    def show_status(self, show_all=False):
        """Show status of a all processes."""
        statii = self.get_status(show_all=show_all)
        if not (
            self.query and "sort=" in self.query and "sort=alpha" not in self.query
        ):
            # I.e. always do this because alpha is the default
            statii = sorted(statii, key=lambda x: x["part"]["process"].lower())
            # Interleave the two halves of the list so the sorting is not split
            # between columns
            statii = chain.from_iterable(
                zip_longest(statii[: len(statii) // 2], statii[len(statii) // 2 :])
            )
            statii = (i for i in statii if i is not None)
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        for status in statii:
            # Create from array
            execute_retry(
                con,
                cur,
                """select * from log where process=? order by timestamp desc limit 100""",
                [status["part"]["process"]],
            )
            out_status = ["HARD" if i[2] == "FAIL" else i[2] for i in cur.fetchall()]
            out_status.reverse()
            mode = "RGB"
            if len(set(out_status)) == 1 and out_status[0] == "OK":
                image_2d = [[0, 0, 0, 0]]
                mode = "RGBA"
            else:
                image_2d = [
                    sum(
                        (self.color_bytes.get(i, [0, 0, 0]) for i in out_status),
                        start=[],
                    )
                ]
            if not image_2d[0]:
                image_2d = [[255, 0, 0, 0, 255, 0, 0, 0, 255]]

            # Save as PNG
            img_data = BytesIO()
            png.from_array(image_2d, mode).write(img_data)
            img_data = img_data.getvalue()
            img_data = "data:image/png;base64," + b64encode(img_data).decode("utf-8")
            status["part"]["img_data"] = img_data
            self.out(
                """<div class='ent'>
                  <div class='tag'>{log_process}</div>
                  <div title='{details}' class='ts {out_status}'>{timestamp}</div>
                  <div class='img-line'>
                    <div class='msg'>{message}</div>
                    <span class='time'>{spare}</span>
                    <br/><img height='2' width='300' src='{img_data}'>
                  </div>
                </div>
                <div class='box {out_status}' title='{process} {details}'>
                  <a href='show/{process}'></a>
                </div> 
                """.format_map(status["part"])
            )

    Field = namedtuple("Field", "name type index", defaults=(None,))
    schema = {
        "process": [
            Field("process", "text", "unique index"),
            Field("interval", "float"),
            Field("description", "text"),
            # ('active', 'boolean'), # ('test', 'test'),
        ],
        "log": [
            Field("process", "text", "index"),
            Field("timestamp", "datetime", "index"),
            Field("status", "text"),
            Field("message", "text"),
            Field("ip", "text"),
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
        "DEFER": "none",
        "DEFER-BOX": "grey",
        "FAIL-BOX": "#4c51a4",
    }
    color_bytes = {
        k: [int(v[1:3], 16), int(v[3:5], 16), int(v[5:7], 16)]
        for k, v in colors.items()
        if v[0] == "#" and len(v) == 7
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
            @media only screen
            and (max-width : 50em) {{
                .ent,.top-menu {{ display: none; }}
            }}
            @media only screen
            and (min-width : 50em) {{
                .box {{ display: none; }}
            }}
            body {{ font-family: sans-serif; font-size: 90%;
                   background: {BACKGROUND}; color: {FOREGROUND}; }}
            .FAIL {{ background: {FAIL}; }}
            .box.FAIL {{ background: {FAIL-BOX}; }}
            .HARD {{ background: {HARD}; }}
            .OK {{ background: {OK}; }}
            .DISABLE {{ background: {DISABLE}; }}
            .ENABLE {{ background: {ENABLE}; }}
            .DEFER {{ background: {DEFER}; }}
            .box.DEFER {{ background: {DEFER-BOX}; }}
            .box {{ float: left; min-width: 3em; min-height: 3em;
                    box-shadow: inset 0 0 10px #0004;
                    position: relative;
            }}
            .box a {{ position: absolute; width: 100%; height: 100%; }}
            .ent {{ float: left; width: 49%; margin-top: 0.5em; alignment-baseline: text-bottom; }}
            .tag {{ display: block; width: 20%; float: left; text-align:right; }}
            .msg {{ float: left; padding-left: 1%;}}
            .ts {{ color: blue; font-size: 75%; float: left; margin: 0 1ex; }}
            .st {{ float: left; color: blue; width: 4em; margin: 0 1ex; }}
            .du {{ float: left; width: 12em; margin: 0 1ex; }}
            .time {{ font-size: 75%; font-style: italic; }}
            div.time {{ clear: left; }}
            a {{ text-decoration: none; color: {FOREGROUND}; }}
            a:active {{ text-decoration: none; color: {FOREGROUND}; }}
            a:visited {{ text-decoration: none; color: {FOREGROUND}; }}
            a:hover {{ text-decoration: underline; color: red }}
            .right {{ text-align: right }}
            .left-side {{ float: left }}
            .right-side {{ float: right }}
            .time {{ float: left; }}
            .img-line {{ float: left; width: 60%; image-rendering: pixelated; }}
            hr {{ border-style: solid; border-color: grey; border-width: 2px 0 0 0 ; }}
            </style>
            <title>Tattle</title>
            </head><body><div class="top-menu">
            <a href="/">Home</a>
            <a href="/?sort=recent">Recent</a>
            <a href="/all">Show disabled</a>
            <a href="/quit">Re-start</a>
            <a href="/update">Get updates</a>
            <a href="/report">Reports</a>
            <hr/></div>""".format(**colors),
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
    """Server setup."""

    pass


def run(server_class=ThreadedServer, handler_class=tattleRequestHandler):
    """Start server."""
    server_address = ("0.0.0.0", 8111)
    httpd = server_class(server_address, handler_class)

    httpd.serve_forever()


if __name__ == "__main__":
    run()
