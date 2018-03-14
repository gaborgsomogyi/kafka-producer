Basic kafka producer application.

```
mvn clean install
```

```
cat << 'EOF' > producer_jaas.conf
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/cdep/keytabs/systest.keytab"
    useTicketCache=false
    serviceName="kafka"
    principal="systest@GCE.CLOUDERA.COM";
}; 
EOF
```

```
java -Djava.security.auth.login.config=producer_jaas.conf -jar target/kafka-producer-0.0.1-SNAPSHOT-jar-with-dependencies.jar <bootstrap-servers>:9093 SASL_SSL topic1 10 1000
```
