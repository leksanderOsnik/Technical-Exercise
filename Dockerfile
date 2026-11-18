# ---- build stage: compile and package with the pinned Maven wrapper ----
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /src
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
# Resolve dependencies in their own layer so source edits don't re-download them.
RUN ./mvnw -B -q dependency:go-offline
COPY src/ src/
# Tests run in CI; the image build only packages.
RUN ./mvnw -B -q -DskipTests package

# ---- runtime stage: JRE only, non-root ----
FROM eclipse-temurin:21-jre-jammy
RUN useradd --system --uid 10001 relay
COPY --from=build /src/target/relay.jar /app/relay.jar
USER relay
EXPOSE 7000
# Exec form: java is PID 1 and receives SIGTERM from `docker stop` -> graceful shutdown.
ENTRYPOINT ["java", "-jar", "/app/relay.jar"]
