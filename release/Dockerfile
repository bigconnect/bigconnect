FROM openjdk:11.0-jre

ENV BIGCONNECT_DIR=/bc
ENV JAVA_OPTS="-Xms4g -Xmx4g -server -XX:+UseG1GC -Dfile.encoding=utf8 -Djava.awt.headless=true"

COPY target/bc-core-4.2.0 /
RUN ln -s /bc-core-4.2.0 /bc
RUN mkdir -p ${BIGCONNECT_DIR}/datastore

VOLUME /bc/datastore

WORKDIR /bc

EXPOSE 10242/tcp
EXPOSE 10243/tcp

CMD java ${JAVA_OPTS} -cp "./lib/*" com.mware.bigconnect.BigConnectRunner
