FROM flink:1.17

# Cài Python3 + pip + PyFlink (client) khớp version
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 python3-pip python3-venv ca-certificates curl \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && pip3 install --no-cache-dir "apache-flink==1.17.*" \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
