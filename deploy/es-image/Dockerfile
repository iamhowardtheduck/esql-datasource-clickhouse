ARG STACK_VERSION=9.5.4
FROM docker.elastic.co/elasticsearch/elasticsearch:${STACK_VERSION}
ARG STACK_VERSION

COPY --chown=elasticsearch:root esql-datasource-clickhouse-${STACK_VERSION}.zip /tmp/esql-datasource-clickhouse.zip
RUN bin/elasticsearch-plugin install --batch file:///tmp/esql-datasource-clickhouse.zip \
    && rm /tmp/esql-datasource-clickhouse.zip
