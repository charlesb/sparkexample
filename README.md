# How to

## Buid jar and submit to yarn

```bash
mvn install -DskipTests
spark-submit --master yarn --deploy-mode client --jars /usr/local/hive/hive-1.1.0-cdh5.8.0/lib/mysql-connector-java-5.1.40-bin.jar,/usr/local/hive/hive-1.1.0-cdh5.8.0/lib/hive-contrib-1.1.0-cdh5.8.0.jar --name sparkExample --class Validation target/spark-example.jar
spark-shell --master yarn --deploy-mode client --jars /usr/local/hive/hive-1.1.0-cdh5.8.0/lib/mysql-connector-java-5.1.40-bin.jar,/usr/local/hive/hive-1.1.0-cdh5.8.0/lib/hive-contrib-1.1.0-cdh5.8.0.jar
```

## Build docker image from Maven with Spring

https://spring.io/guides/gs/spring-boot-docker/

```bash
mvn install dockerfile:build
docker run -p 8080:8080 -t springio/spark-example
```

visit [http://localhost:8080](http://localhost:8080)
