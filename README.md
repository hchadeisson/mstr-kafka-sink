# mstr-kafka-sink
mstr-kafka-sink is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect) plugin for sending data from Kafka to a MicroStrategy server.
The data residing in MicroStrategy is stored in memory, optimized for Dashboard creation and fast access to the real time data stream.

## Prerequisites
The information below assumes you have at least MicroStrategy 10.9, Kafka and Kafka Connect deployed.

Configuring the connector requires at least the following information:
* MicroStrategy Library url endpoint
* MicroStrategy username
* MicroStrategy password
* MicroStrategy Project Name

This connector only supports pushing a JSON topic to a MicroStrategy Cube. Other formats (TXT, CSV, etc.) need to be transformed in Kafka prior to be pushed to MicroStrategy. [KSQL](https://www.confluent.io/product/ksql/) can help you do this

## Installing
### Compiling the connector
Clone this repository, open the project in eclipse and compile the connector:
* Double click on "Connect_Sink.jardesc"
* In the popup that opens, update the target Jar file location
* Hit the "Finish" button
* Right click on "Connect_Sink.jardesc"
* From the menu, select "Create JAR"

### Deploy the connector
The resulting jar file should go in the Plugins folder of Kafka Connect.
If you're using Docker to run it, make sure to have a Shared folder with your server that contains the Jar and is properly mapped to the ```CONNECT_PLUGIN_PATH``` parameter

### All done
You should be good to go, just restart Kafka Connect and let's configure the Sink

## Configure the Sink
2 options are available to configure the Sink. You can either create it using a REST API Call on Kafka Connect, or if you are using [Confluent.io](http://confluent.io/), use the [Control Center](https://www.confluent.io/confluent-control-center/) to create it from a user friendly web portal.

### Using REST API Call
Update bold parameters

> curl -XPOST -H 'Accept: application/json' -H "Content-type: application/json" -d '{"name": "**MicroStrategySinkTest**","config": {"connector.class": "com.microstrategy.se.kafka.pushapi.MicroStrategySink", "topics": "**users**","CONFIG_LIBRARY_URL": "**yourserver-url/MicroStrategyLibrary**","CONFIG_USER": "**mstr**","CONFIG_PASSWORD": "**yourpassword**", "CONFIG_PROJECT": "**MicroStrategy Tutorial**","CONFIG_CUBE": "**MicroStrategy & Kafka**"}}' '**kafka-machine:8083/connectors**'

### Using Confluent.io Control Center
* Open Confluent Control Center
* Go to Kafka Connect
* Select `SINKS` and click the `+ New Sink` button
* Choose the Topic to send to MicroStrategy and click **Continue**
* In the next screen, select **MicroStrategySink** connector class and set the connector name, that will show up in the Kafka Connect list of Sinks
* A new section appears, scroll all the way down to the MicroStrategy subsection
* Fill all the fields with the required information listed in the prerequisites

# Special thanks
To Alex Fernandez, who implemented this first and passed along the knowledge so we could reuse it
