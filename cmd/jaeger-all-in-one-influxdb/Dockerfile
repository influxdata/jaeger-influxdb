FROM alpine:3.10.1

ARG JAEGER_VERSION=1.24.0
ARG INFLUXDB_VERSION=1.7.7
ARG CHRONOGRAF_VERSION=1.7.12

# Supervisord
RUN set -ex && \
    apk add --no-cache wget tar supervisor && \
    mkdir -p /etc/supervisor

# Jaeger
RUN set -ex && \
    mkdir -p /usr/src && \
    wget --no-verbose https://github.com/jaegertracing/jaeger/releases/download/v${JAEGER_VERSION}/jaeger-${JAEGER_VERSION}-linux-amd64.tar.gz && \
    tar -C /usr/src -xzf jaeger-${JAEGER_VERSION}-linux-amd64.tar.gz && \
    mv /usr/src/jaeger-${JAEGER_VERSION}-linux-amd64/jaeger-all-in-one /usr/src/jaeger-${JAEGER_VERSION}-linux-amd64/example-hotrod /usr/local/bin/ && \
    rm -rf jaeger-${JAEGER_VERSION}-linux-amd64.tar.gz /usr/src && \
    mkdir -p /etc/jaeger

# InfluxDB
RUN set -ex && \
    mkdir -p /usr/src && \
    wget --no-verbose https://dl.influxdata.com/influxdb/releases/influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz && \
    tar -C /usr/src -xzf influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz && \
    rm -f /usr/src/influxdb-*/influxdb.conf && \
    mv /usr/src/influxdb-*/influx* /usr/local/bin/ && \
    rm -rf influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz /usr/src && \
    mkdir -p /etc/influxdb /var/lib/influxdb

# Chronograf
RUN set -ex && \
    mkdir -p /usr/src && \
    wget --no-verbose https://dl.influxdata.com/chronograf/releases/chronograf-${CHRONOGRAF_VERSION}-static_linux_amd64.tar.gz && \
    tar -C /usr/src -xzf chronograf-${CHRONOGRAF_VERSION}-static_linux_amd64.tar.gz && \
    mv /usr/src/chronograf-*/* /usr/local/bin/ && \
    rm -rf chronograf-${CHRONOGRAF_VERSION}-static_linux_amd64.tar.gz /usr/src && \
    mkdir -p /var/lib/chronograf

COPY ./cmd/jaeger-all-in-one-influxdb/supervisord.conf /etc/supervisor

COPY ./cmd/jaeger-all-in-one-influxdb/run_jaeger.sh /usr/local/bin
COPY ./cmd/jaeger-all-in-one-influxdb/sampling_strategies.json /etc/jaeger
COPY ./cmd/jaeger-influxdb/jaeger-influxdb-linux /usr/local/bin

COPY ./cmd/jaeger-all-in-one-influxdb/influxdb.conf /etc/influxdb

COPY ./cmd/jaeger-all-in-one-influxdb/chronograf.bolt /var/lib/chronograf

# Agent zipkin.thrift compact
EXPOSE 5775/udp
# Agent jaeger.thrift compact
EXPOSE 6831/udp
# Agent jaeger.thrift binary
EXPOSE 6832/udp
# Agent config HTTP
EXPOSE 5778
# Collector HTTP
EXPOSE 14268
# Collector gRPC
EXPOSE 14250
# Web HTTP
EXPOSE 16686
# InfluxDB
EXPOSE 8086
# Chronograf
EXPOSE 8888
# Hotrod
EXPOSE 8080

ENV SPAN_STORAGE_TYPE=grpc-plugin \
    GRPC_STORAGE_PLUGIN_BINARY=/usr/local/bin/jaeger-influxdb-linux \
    SPAN_STORAGE_TYPE=grpc-plugin \
    INFLUXDB_HOST=http://localhost:8086 \
    INFLUXDB_DATABASE=tracing \
    INFLUXDB_RETENTION_POLICY=tracing \
    INFLUXDB_RETENTION_HOURS=24 \
    INFLUXDB_SHARD_DURATION_HOURS=1

ENTRYPOINT supervisord -n -c /etc/supervisor/supervisord.conf
