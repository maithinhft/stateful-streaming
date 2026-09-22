FROM alpine:latest

RUN apk add --no-cache krb5-server krb5 bash

EXPOSE 88 88/udp 749

ENTRYPOINT ["/bin/sh", "/usr/local/bin/init-kdc.sh"]