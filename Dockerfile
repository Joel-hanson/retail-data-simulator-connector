FROM maven:3-ibm-semeru-17-focal AS build
WORKDIR /app
COPY pom.xml pom.xml
COPY src ./src
RUN mvn clean package

FROM apache/kafka:4.0.0
WORKDIR /opt/kafka
RUN mkdir -p /opt/kafka/plugins/kafka-connector-template
COPY --from=build /app/target/*.jar /opt/kafka/plugins/kafka-connector-template/
RUN mkdir -p /tmp/config
COPY config/connect-distributed.properties /tmp/config/