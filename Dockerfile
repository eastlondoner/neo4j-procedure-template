FROM debian:stable-slim

COPY ./target/ssr-plugin-1.0.0-SNAPSHOT.jar /neo4j-plugins/ssr-plugin-1.0.0-SNAPSHOT.jar
RUN chown -R 7474 /neo4j-plugins

ENV DESTINATION=/plugins

ENTRYPOINT ["sh", "-c", "cp /neo4j-plugins/* \"${DESTINATION}\"" ]
