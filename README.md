# Flink sample with Kafka
## Starting the project
### Requirements to compile
* MVN
* Java
### Requirements to Test
* Flink
* Kafka
### Create the project in MAVEN
This is only if you want to create new code. I started with the following. 
```bash
mvn archetype:generate                               \
     -DarchetypeGroupId=org.apache.flink              \
     -DarchetypeArtifactId=flink-quickstart-java      \
     -DarchetypeVersion=1.9.0
```
### Compile the Flink Job
```bash
mvn clean package
```

### Run the Flink Job
Please, go to the target folder, you will see the result of the compilation: webtraffic-by-country-1.0-SNAPSHOT.jar.
To test it, you need Kafka, in my case is running a cluster of 3 brokers with a Zookeeper on another 3 nodes. Topic is WebTraffic.
```bash
flink run webtraffic-by-country-1.0-SNAPSHOT.jar --topic WebTraffic --bootstrap.servers hgalante-cdf-5.gce.cloudera.com:9092,hgalante-cdf-6.gce.cloudera.com:9092,hgalante-cdf-7.gce.cloudera.com:9092 --group.id Flink --timeWindow 15 --zookeeper.servers hgalante-cdf-2.gce.cloudera.com:2181,hgalante-cdf-3.gce.cloudera.com:2181,hgalante-cdf-4.gce.cloudera.com:2181
```
To see the results, just go to the Task manager/ Click on the task manager and go to the Stdout window to the see the results.

![Flink TaskManager](images/TaskManager.png)

![Stdout Results](images/Stdout.png)





