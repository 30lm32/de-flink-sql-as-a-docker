FROM flink:1.9.0-scala_2.11

RUN mkdir app
RUN mkdir -p ./app/target

WORKDIR app
COPY ./run.sh ./run.sh

CMD ["/bin/sh", "./run.sh"]