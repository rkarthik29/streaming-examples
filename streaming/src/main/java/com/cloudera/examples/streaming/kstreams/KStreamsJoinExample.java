package com.cloudera.examples.streaming.kstreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class KStreamsJoinExample {
    public static void main(String[] args) {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name. The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "shipment-join");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "knarayanan0.field.hortonworks.com:6667");

        // Specify default (de)serializers for record keys and for record values.

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> shipping = builder.table("shippingJson");
        final KTable<String, String> orders = builder.table("ordersJson");

        // orders.foreach(new ForeachAction<String, String>() {
        //
        // @Override
        // public void apply(String key, String value) {
        //
        // System.out.println(key + ":" + value);
        //
        // }
        // });

        KTable<String, String> confirmation = orders.join(shipping, new ValueJoiner<String, String, String>() {

            @Override
            public String apply(String value1, String value2) {
                // TODO Auto-generated method stub
                JSONParser parser = new JSONParser();
                String data = value1;
                try {
                    JSONObject orders = (JSONObject) parser.parse(value1);
                    JSONObject shipping = (JSONObject) parser.parse(value2);
                    orders.put("shipmentid", shipping.get("shipmentid"));
                    orders.put("shipday", shipping.get("shipday"));
                    data = orders.toJSONString();
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return data;
            }
        });

        confirmation.toStream().to("confirmations");
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
