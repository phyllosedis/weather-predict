FROM sbtscala/scala-sbt:eclipse-temurin-21.0.5_11_1.10.7_3.6.2 AS builder
WORKDIR /app

COPY build.sbt .
COPY project ./project
RUN sbt update

COPY src ./src
RUN sbt compile

CMD ["sbt", "run"]
