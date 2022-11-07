FROM adoptopenjdk/maven-openjdk11

COPY src/ /home/ubuntu/
COPY pom.xml /home/ubuntu/

RUN cd /home/ubuntu && mvn clean install

