# To DEBUG, do:
# docker build . -t redpipe && docker run -it redpipe /bin/bash

# To RUN, do:
# docker build . -t redpipe && docker run redpipe



FROM themattrix/tox-base

COPY requirements.txt /app/
COPY dev-requirements.txt /app/
COPY tox.ini /app/
COPY setup.py /app/
COPY README.rst /app/
COPY MANIFEST.in /app/
COPY README.rst /app/
COPY LICENSE /app/
COPY test.py /app/
COPY docs /app/docs/
COPY redpipe /app/redpipe/
