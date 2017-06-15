FROM phusion/baseimage:0.9.19
MAINTAINER Brad Wasson "bwasson@mediamath.com"

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]

WORKDIR /opt/qasino

ADD bin/qasino-server /opt/qasino/bin/qasino-server
ADD etc/files /opt/qasino/etc/files
ADD etc/htdocs /opt/qasino/etc/htdocs

ADD etc/config.yaml.sample /opt/qasino/etc/config.yaml

RUN mkdir /etc/service/qasino
RUN mkdir /opt/qasino/log

ADD docker/runit/qasino-server.sh     /etc/service/qasino-server/run
ADD docker/runit/qasino-server-log.sh /etc/service/qasino-server/log/run

RUN chmod 755 /etc/service/qasino-server/run
RUN chmod 755 /etc/service/qasino-server/log/run

# Enable SSHD for login access
RUN rm -f /etc/service/sshd/down

# Generate a self-signed cert for testing only. Replace this with proper certs.
RUN openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/C=US/ST=Denial/L=Somewhere/O=Dis/CN=www.nowhere.com"

EXPOSE 80
EXPOSE 443
EXPOSE 15000
EXPOSE 15596
EXPOSE 15597
EXPOSE 15598

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

