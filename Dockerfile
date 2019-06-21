FROM openjdk:8-jre-slim

WORKDIR /app
COPY target/KVRawClientTest-0.1.0-SNAPSHOT-jar-with-dependencies.jar /app/app.jar

COPY golden-path-tutorial-6522-a0243d98c0a0.json /app/key.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/key.json

CMD ["java", "-jar", "/app/app.jar"]