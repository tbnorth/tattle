Single file zero dependency status reporting web app.

Logs status messages and displays good / overdue / failed status.

(old code recently ported to Python 3)

## Example `tattle_update` command for Docker set-up

```shell
#!/bin/sh

python /sitemon/sitemon.py sitemon.yaml >reports/site_status.html
```

## Some docker set commands

```shell
docker build -t tattle_sitemon .
# live code paths below for dev only
docker run --restart unless-stopped -d --name tattle_sitemon \\
  -p 8111:8111 \\
  -v $PWD/data:/data \\
  -v /home/me/path/to/live/code/sitemon:/sitemon \\
  -v /home/me/path/to/live/code/tattle:/tattle \\
  tattle_sitemon
docker attach tattle_sitemon
docker rm -f tattle_sitemon
docker logs -f tattle_sitemon
```
