FROM java:8u102-jdk

RUN mkdir -p /opt/kafka

ADD /build/libs/client-connect-0.1.0.jar /opt/kafka/client-connect-0.1.0.jar

ADD run.sh /opt/kafka/

RUN chmod 777 /opt/kafka/client-connect-0.1.0.jar

RUN chmod 777 /opt/kafka/run.sh

EXPOSE 9093

CMD ["/opt/kafka/run.sh"]