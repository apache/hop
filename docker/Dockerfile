FROM tomcat:jdk8
MAINTAINER Hiromu Hota <hiromu.hota@hal.hitachi.com>

ADD hop.war webapps/
ADD hop-assemblies-plugins-dist.zip /tmp/
RUN unzip -q /tmp/hop-assemblies-plugins-dist.zip
