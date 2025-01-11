#FROM dk-reg.mas:5000/openjdk:21-jdk-slim
#FROM dk-reg.mas:5000/wildfly/wildfly:latest-jdk21

FROM openjdk:21-jdk-slim

RUN mkdir -p /opt/jboss/wildfly/logs

ENV TZ="Asia/Kolkata"
EXPOSE 20000/tcp

RUN mkdir -p /opt/jboss/wildfly/logs




COPY ./target/common/log4j2-common.xml /log4j2-common.xml



COPY ./target/profile/digitalocean1/req_receiver.properties_do1 /req_receiver.properties_do1



COPY ./target/profile/digitalocean2/req_receiver.properties_do2 /req_receiver.properties_do2



COPY ./target/profile/digitalocean3/req_receiver.properties_do3 /req_receiver.properties_do3

COPY ./target/queryprocessor-main-module-1.0-executable.jar /queryprocessor-main-module-1.0-executable.jar

ENTRYPOINT ["java","-Dlog4j.configurationFile=file:/log4j2-common.xml","-jar","/queryprocessor-main-module-1.0-executable"]

