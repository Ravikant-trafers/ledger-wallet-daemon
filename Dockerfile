### BUILD STEP ###
FROM adoptopenjdk:8u272-b10-jdk-openj9-0.23.0-focal as builder
ARG COMMIT_HASH=""
ENV STAGE dev
ENV COMMIT_HASH $COMMIT_HASH

WORKDIR /build
ADD . /build
RUN ./docker/build.sh

#### RUN STEP ###
FROM adoptopenjdk:8u272-b10-jre-openj9-0.23.0
ENV HTTP_PORT 9200
ENV ADMIN_PORT 0
ENV STAGE dev

WORKDIR /app
COPY --from=builder /build/target/universal/stage .
COPY ./docker/install_run_deps.sh .
COPY ./docker/run.sh .
RUN ./install_run_deps.sh && rm -f install_run_deps.sh

CMD ["/app/run.sh"]
