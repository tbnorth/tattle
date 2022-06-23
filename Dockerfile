FROM python:3.9-alpine

RUN apk update \
 && apk add bash git

WORKDIR '/data'

# put versions in /data for reference
ADD https://api.github.com/repos/tbnorth/sitemon/git/refs/heads/dev version.sitemon.json
ADD https://api.github.com/repos/tbnorth/tattle/git/refs/heads/dev version.tattle.json

RUN git clone https://github.com/tbnorth/sitemon.git /sitemon \
 && git clone https://github.com/tbnorth/tattle.git /tattle \
 && cd /sitemon \
 && pip install -r requirements.txt

ENV PATH="/data:${PATH}"

CMD ["python", "/tattle/tattle.py"]
