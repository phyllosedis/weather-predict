# Этап 1: Сборка WAR-архива
FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY pom.xml .
COPY orchestrator/pom.xml ./orchestrator/
COPY orchestrator/src ./orchestrator/src
RUN mvn clean package -pl orchestrator -DskipTests

# Этап 2: Запуск на Open Liberty (Java 25)
FROM icr.io/appcafe/open-liberty:full-java25-openj9-ubi-minimal
COPY --chown=1001:0 orchestrator/src/main/liberty/config/server.xml /config/
COPY --chown=1001:0 --from=builder /build/orchestrator/target/orchestrator.war /config/apps/
