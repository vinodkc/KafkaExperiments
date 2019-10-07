# KafkaSecureClientDemo

1) SASL Producer Demo:

java -Djava.security.auth.login.config=./kafka_client_jaas.conf -Djava.security.krb5.conf=./krb5.conf -cp ./target/KafkaSecureClientDemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.vkc.SecureProducer  testacl hdp265secure3.openstacklocal:6667 1000

2) SASL Consumer Demo:

java -Djava.security.auth.login.config=./kafka_consumer_jaas.conf -Djava.security.krb5.conf=./krb5.conf -cp ./target/KafkaSecureClientDemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.vkc.SecureConsumer  testacl hdp265secure3.openstacklocal:6667


3)SASL_SSL Producer Demo:

java -Djava.security.auth.login.config=./kafka_client_jaas.conf -Djava.security.krb5.conf=./krb5.conf -cp ./target/KafkaSecureClientDemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.vkc.SecureSSLProducer  testacl hdp265secure3.openstacklocal:6668 10 ./client.truststore.jks test1234

4)SASL_SSL Consumer Demo

java -Djava.security.auth.login.config=./kafka_consumer_jaas.conf -Djava.security.krb5.conf=./krb5.conf -cp ./target/KafkaSecureClientDemo-1.0-SNAPSHOT-jar-th-dependencies.jar com.vkc.SecureSSLConsumer  testacl hdp265secure3.openstacklocal:6668  ./client.truststore.jks test1234


more details : https://gist.github.com/vinodkc/b99ba4ddc159b53c6b2c89f1d8125573
