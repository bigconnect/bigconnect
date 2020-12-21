###################
# STAGE 1: builder
##################
FROM maven:3.6-jdk-11 as builder

WORKDIR /source

COPY . .

RUN mvn -Pbin-release -DskipTests install

###################
# STAGE 2: runner
##################
FROM adoptopenjdk/openjdk11:alpine-jre as runner

ENV BIGCONNECT_DIR=/bc
ENV JAVA_OPTS="-Xms4g -Xmx4g -server -XX:+UseG1GC -Dfile.encoding=utf8 -Djava.awt.headless=true"
RUN mkdir -p ${BIGCONNECT_DIR}/datastore

COPY --from=builder /source/release/target/bc-core-4.2.0/bc-core-4.2.0 ${BIGCONNECT_DIR}

VOLUME /bc/datastore

WORKDIR /bc

EXPOSE 10242/tcp
EXPOSE 10243/tcp

CMD java ${JAVA_OPTS} -cp "./lib/*" com.mware.bigconnect.BigConnectRunner
