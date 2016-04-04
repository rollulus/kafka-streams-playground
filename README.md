Kafka Streams Playground
========================

Here are a few examples I made while experimenting with the Kafka Streams.
At the moment of writing (April 2016), Confluent provides a tech preview version of Kafka Streams in their 2.1.0-alpha version of their platform.
I hope that my examples help others getting getting started.

My examples source from the [Twitter Kafka Connector](https://github.com/Eneco/kafka-connect-twitter).
AVRO serialized `TwitterStatus` are used as input.

Usage
=====

    mvn package
    java -cp target/kafka-streams-playground-0.1-jar-with-dependencies.jar com.github.rollulus.myprocessor.TweetsPerMinuteEventTime tweets-per-user-counter.properties

To run the `TweetsPerMinuteCounter` example. Despite the name, `tweets-per-user-counter.properties` can be used for all examples. 

Example Stream Processors
=========================

TweetsPerUserCounter
--------------------

Gives an unbounded count of tweets per user.
The key is the username, the value is a long.

TweetsPerMinuteProcessingTime
-----------------------------

Counts tweets per minute at processing time, using `HoppingWindows`.

TweetsPerMinuteEventTime
------------------------

Counts tweets per minute at event time (i.e. the timestamp assigned by Twitter).

