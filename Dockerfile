FROM sbtscala/scala-sbt:eclipse-temurin-25.0.3_9_1.12.12_3.8.4 AS builder

# Copy the project sources
COPY . /app

# Build the project
WORKDIR /app
RUN sbt assembly

# Create the final image
FROM eclipse-temurin:25-jre-noble
LABEL maintainer="Piotr Sowiński <piotr@neverblink.eu>"

RUN apt-get update && \
    apt-get install -y git wget && \
    rm -rf /var/lib/apt/lists/*

# Copy the executable jar
COPY --from=builder /app/target/assembly/ci-worker-assembly.jar /app/
COPY bin/ci-worker /usr/local/bin/ci-worker
RUN chmod +x /usr/local/bin/ci-worker

WORKDIR /worker
ENTRYPOINT []
