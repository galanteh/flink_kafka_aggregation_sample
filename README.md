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

# How to install Flink

## Apache Flink
if you have a mac, with brew you can install it as easy as:
```bash
# Install brew
brew install flink
# Go to the Flink directory and start the cluster
$ ./bin/start-cluster.sh  # Start Flink
# Now you can start a JAR job with the flink command like
flink run /pathto/SocketWindowWordCount.jar --port 9000
```
If you point to http://localhost:8081 you will access to the Flink Web Dashboard

## Flink on Cloudera
if you are using a Cloudera cluster, you will need YARN and YARN required HDFS. After installing that, you need the Flink parcel from the URL's provided by [Cloudera Downloads](https://www.cloudera.com/downloads) on the DataFlow section. 
After restarting Cloudera Manager, you will have the Service, and you can deploy it over that Cluster. 

### Flink Service
![Flink Service](images/Flink_Service.png)

### Flink Configuration
![Flink Configuration](images/Flink_Configuration.png)

### Flink Role Distribution
![Flink Configuration](images/Flink_Role_Distribution.png)

#### Trobleshouting
If you have a new fresh installation, maybe you need to create some default directories on HDFS for Flink to avoid some start issues with directories permissions on the HDFS.

```bash
sudo -u hdfs hadoop fs -mkdir -p /user/flink/applicationHistory
sudo -u hdfs hadoop fs -mkdir -p /user/flink/checkpoints
sudo -u hdfs hadoop fs -chown -R flink:flink /user/flink
sudo -u hdfs hadoop fs -chmod -R 1777 /user/flink
```

# Running an Standart demo
On the path /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming of the Linux boxes where Flink is installed you can find some standart demos from Flink.
```bash
ls /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming
```
One nice demo is the is the SocketWindowWordCount.jar where you can enter a text into the console and Flink will analize the Words and will count them into repetition groups. This is a nice sample to demo to new people. 
```bash
nc -lk 9000
flink run path_to/SocketWindowWordCount.jar --port 9000
```
