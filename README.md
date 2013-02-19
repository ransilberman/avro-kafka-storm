Avro Kafka Storm Integration
============================

This is a demo code that shows how to use a Kafka messaging where the message format is Avro.
It uses Storm topology as the consumer of Kafka using KafkaSpout.
The user story of this test is about event streaming from the Internet that we want to process in Storm.


### Building from Source

$ mvn install

### Usage
The project contains two modules: one is AvroMain which is the Kafka Producer test application.
Second is AvroStorm which is the Storm Topology code

**Basic Usage**
`LPEvent.avsc` is the Avro Schema. It exists in both projects.

`MainTest` runs three tests, each of which sends event to Kafka.
You may need to change the variable 'zkConnection' to your own zookeeper servers list that hold Kafka service brokers.

`AvroTopology` is the class that runs the Storm Topology.
You may need to change the host list zookeeper servers list that holds Kafka service brokers.

First start the topology, then run the tests and the events will pass via Kafka in Avro format

**First Test**
Send event with GenericRecord class.

**Second Test**
Build Avro object using Generated Resources.
This test will compile only after the code is generated using the Maven plugin 'avro-maven-plugin'.

**Third Test**
Serializes the Avro objects into file and then reads the file.

**upgrade Avro scema version**
`AvroTopology2` is a second Storm topology that demonstrates how to use a schema that differs between producer and consumer.
The difference in the code is in the line:
$ DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema, _newSchema);
Note that there are two parameters to the GenericDatumReader constructor.