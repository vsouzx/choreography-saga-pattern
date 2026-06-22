FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /app

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

RUN chmod +x mvnw && ./mvnw dependency:go-offline -B

COPY src src

RUN ./mvnw package -DskipTests -B

FROM eclipse-temurin:21-jre-alpine

RUN apk add --no-cache tzdata

WORKDIR /app

COPY --from=builder /app/target/*.jar app.jar

EXPOSE 8082

CMD ["java", "-jar", "app.jar"]
