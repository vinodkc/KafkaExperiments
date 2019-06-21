###KafkaStreamDemo

Demo program to convert messages to upper case using kafka stream

**Build the project**

cd ./KafkaExperiments/kafkastreamsDemo1

mvn package

**Run**

`java -jar ./target/kafkastreamsDemo1-1.0-SNAPSHOT-jar-with-dependencies.jar localhost:9092  sourcetopic  destTopic`


**Produce some data to source topic**

`./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sourcetopic`


**Consume from  desttopic**

`./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic destTopic`