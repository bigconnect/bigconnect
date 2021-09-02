FROM adoptopenjdk/openjdk11:alpine-jre

ENV BIGCONNECT_DIR=/bc
ENV JAVA_OPTS="-Xms4g -Xmx4g -server -XX:+UseG1GC -Dfile.encoding=utf8 -Djava.awt.headless=true"
RUN mkdir -p ${BIGCONNECT_DIR}/datastore

COPY release/target/bc-core/bc-core ${BIGCONNECT_DIR}

VOLUME /bc/datastore

WORKDIR /bc

EXPOSE 10242/tcp
EXPOSE 10243/tcp

CMD java ${JAVA_OPTS} -cp "./lib/*" com.mware.bigconnect.BigConnectRunner
