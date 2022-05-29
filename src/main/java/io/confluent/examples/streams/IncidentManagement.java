/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.common.utils.TestUtils;
import io.confluent.examples.streams.message.AgentMessage;
import io.confluent.examples.streams.serviceproviders.DummyIncidentManagementServiceProvider;
import io.confluent.examples.streams.serviceproviders.IncidentManagementService;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Anomaly detection Threshold based
 * }</pre>
 */
public class IncidentManagement {

  static final String inputTopic = "incident-management";
  static final String outputTopic = "shadow-topic-unused";
  static final Logger logger = Logger.getLogger(IncidentManagement.class);
  /**
   * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
   */
  public static void main(final String[] args) {
    //TODO: parameterized other configuration for more control
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String inputTopicArg = args.length > 1 ? args[1] : inputTopic;
    final String outputTopicArg = args.length > 2 ? args[1] : outputTopic;
    // Configure the Streams application.
    final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers);

    // Define the processing topology of the Streams application.
    final StreamsBuilder builder = new StreamsBuilder();
    createAnomalyDetectionStream(builder, inputTopicArg, outputTopicArg);
    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to play around with the example
    // when resetting the application for doing a re-run (via the Application Reset Tool,
    // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.
    streams.cleanUp();

    // Now run the processing topology via `start()` to begin processing its input data.
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  /**
   * Configure the Streams application.
   *
   * Various Kafka Streams related settings are defined here such as the location of the target Kafka cluster to use.
   * Additionally, you could also define Kafka Producer and Kafka Consumer settings when needed.
   *
   * @param bootstrapServers Kafka cluster address
   * @return Properties getStreamsConfiguration
   */
  static Properties getStreamsConfiguration(final String bootstrapServers) {
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "incident-management");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "incident-management-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    // For illustrative purposes we disable record caches.
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
    return streamsConfiguration;
  }

  /**
   * Define the processing topology for Incident Management
   *
   * @param builder StreamsBuilder to use
   */
  static void createAnomalyDetectionStream(final StreamsBuilder builder, String inputTopic, String outputTopic) {

    final KStream<String, String> incidentMessageStream = builder.stream(inputTopic);

    ObjectMapper objectMapper = new ObjectMapper();

    //TODO: inject it at runtime using a factory
    IncidentManagementService<AnomalyDetection.IncidentMessage, String> incidentManagementService = new DummyIncidentManagementServiceProvider();

    incidentMessageStream.mapValues((v) -> {
      AnomalyDetection.IncidentMessage incidentMessage = null;
      try {
        incidentMessage = objectMapper.readValue(v, AnomalyDetection.IncidentMessage.class);
      } catch (JsonProcessingException e) {
        logger.error( "Error occured while passing Incident Message:" + e.getMessage());
        throw new RuntimeException(e);
      }
      return incidentMessage;
    }).mapValues((v) -> {
      String response = null;
      if(!incidentManagementService.checkDuplicateIncident(v)) {
        response = incidentManagementService.raiseIncident(v);
      } else {
        response = "Duplicate Incident found";
      }
      return response;
    });


  }

}
