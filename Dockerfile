# GraalVM CE 22.3.0-b2 Java 17, Scala 3.2.2, SBT 1.8.2
FROM sbtscala/scala-sbt:graalvm-ce-22.3.0-b2-java17_1.8.2_3.2.2 as builder

# Copy the project sources
COPY . /app

# Build the project
WORKDIR /app
RUN sbt assembly

# Create the final image
FROM eclipse-temurin:19-jre-jammy
MAINTAINER "Piotr Sowiński <piotr.sowinski@ibspan.waw.pl>"

RUN apt update && \
    apt install -y git wget && \
    rm -rf /var/lib/apt/lists/*

# Copy the executable jar
COPY --from=builder /app/target/assembly/ci-worker-assembly.jar /app/
COPY bin/ci-worker /usr/local/bin/ci-worker
RUN chmod +x /usr/local/bin/ci-worker

WORKDIR /worker
ENTRYPOINT []
