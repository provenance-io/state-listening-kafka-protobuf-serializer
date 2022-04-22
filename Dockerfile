FROM gradle:7.4.2-jdk11 as build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build --no-daemon > /dev/null 2>&1 || true

FROM openjdk:11-jre-slim
RUN mkdir /app
COPY --from=build /home/gradle/src/CHANGELOG.md /app
COPY --from=build /home/gradle/src/CODE_OF_CONDUCT.md /app
#COPY --from=build /home/gradle/src/CONTRIBUTING.md /app
COPY --from=build /home/gradle/src/LICENSE /app
COPY --from=build /home/gradle/src/README.md /app
COPY --from=build /home/gradle/src/build/libs/*.jar /app/state-listening-kafka-protobuf-serializer.jar
CMD ["java", "-jar", "/app/state-listening-kafka-protobuf-serializer.jar"]
