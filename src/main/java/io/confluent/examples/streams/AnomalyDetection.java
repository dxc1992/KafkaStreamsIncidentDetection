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
public class AnomalyDetection {

  static final String inputTopic = "streams-plaintext-input";
  static final String outputTopic = "streams-wordcount-output";
  static final Logger logger = Logger.getLogger(AnomalyDetection.class);
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
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detection");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "anomaly-detection-client");
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

  public static class IncidentMessage {

    private String source;
    private Long anomalyCount;

    private String tenantName;

    private String serviceName;

    private String instanceName;

    private Long startTimestamp;

    private Long endTimestamp;

    public IncidentMessage(String sourceAsString, Long currentValueAsLong, String tenantName, String serviceName, String instanceName, Long startTimestamp, Long endTimestamp) {
      this.anomalyCount = currentValueAsLong;
      this.source = sourceAsString;
      this.tenantName = tenantName;
      this.serviceName = serviceName;
      this.instanceName = instanceName;
      this.startTimestamp = startTimestamp;
      this.endTimestamp = endTimestamp;
    }

    public String getTenantName() {
      return tenantName;
    }

    public void setTenantName(String tenantName) {
      this.tenantName = tenantName;
    }

    public String getServiceName() {
      return serviceName;
    }

    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }

    public String getInstanceName() {
      return instanceName;
    }

    public void setInstanceName(String instanceName) {
      this.instanceName = instanceName;
    }

    @Override
    public String toString() {
      return super.toString();
    }

    public String toString(ObjectMapper objectMapper) throws JsonProcessingException {
      return objectMapper.writeValueAsString(this);
    }

    public String getSource() {
      return source;
    }

    public void setSource(String source) {
      this.source = source;
    }

    public Long getAnomalyCount() {
      return anomalyCount;
    }

    public void setAnomalyCount(Long anomalyCount) {
      this.anomalyCount = anomalyCount;
    }

    public Long getStartTimestamp() {
      return startTimestamp;
    }

    public void setStartTimestamp(Long startTimestamp) {
      this.startTimestamp = startTimestamp;
    }

    public Long getEndTimestamp() {
      return endTimestamp;
    }

    public void setEndTimestamp(Long endTimestamp) {
      this.endTimestamp = endTimestamp;
    }
  }

  /**
   * Define the processing topology for Anomoly
   *
   * @param builder StreamsBuilder to use
   */
  static void createAnomalyDetectionStream(final StreamsBuilder builder, String inputTopic, String outputTopic) {

    final KStream<String, String> batchTimeSeriesLogs = builder.stream(inputTopic);
    long threshold = 50;
    ObjectMapper objectMapper = new ObjectMapper();
    //TODO: move decouple anomaly detection logic into separate class
    // inject Anomaly detection class at runtime to switch implementations
    // take in log type as input
    //TODO: add compression decompression to save bandwidth if required
    List<String> anomalyWordList = Arrays.asList("error", "exception", "critical", "fatal");
    StringBuilder regexp = new StringBuilder();
    for (String word : anomalyWordList) {
      regexp.append("(?=.*").append(word).append(")|");
    }
    String cleanRegexpStr = regexp.substring(0, regexp.length() - 1);
    Pattern anomalyPattern = Pattern.compile(cleanRegexpStr);
    final Pattern newLinePattern = Pattern.compile("\\R", Pattern.UNICODE_CHARACTER_CLASS);
    batchTimeSeriesLogs.map((k,v) -> new KeyValue<>(k, new ValueWithSource(v, null)))
             .mapValues( (value) -> {
               String _source = value.getSourceAsString();
               AgentMessage agentMessage = null;
               try {
                 agentMessage = objectMapper.readValue(_source, AgentMessage.class);
               } catch (JsonProcessingException e) {
                 logger.info("Error occurred reading AgentMessage: " + e.getMessage());
                 throw new RuntimeException(e);
               }

               List<String> newValues = Arrays.asList(newLinePattern.split(agentMessage.getLogMessage().toLowerCase()) );
               long errorCount = newValues.stream().filter(v -> anomalyPattern.matcher(v).find()).count();
               ValueWithSource result = new ValueWithSource(agentMessage, errorCount);
               return result;
             } )
            .filter( (k,v)-> v.getCurrentValueAsLong() > threshold ).mapValues((v) -> {
              //TODO: Parameterized threshold
              logger.info("Threshold passed:" + v.getCurrentValueAsLong());
              try {
                AgentMessage agentMessage = v.getSourceAsClassType(AgentMessage.class);
                String message = new IncidentMessage(agentMessage.getLogMessage(), v.getCurrentValueAsLong(), agentMessage.getTenantName(), agentMessage.getServiceName(), agentMessage.getInstanceName(), agentMessage.getStartTimestamp(), agentMessage.getEndTimestamp())
                        .toString(objectMapper);
                logger.info("Output topic:" + outputTopic);
                return message;
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            }).mapValues((v) -> {
              //logger.info("Write To Topic:" + v);
              return v;
            })
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

      //TODO: Consume IncidentMessage and process it to create Incidents
      //TODO: handle duplicate incidents
  }

  public static class ValueWithSource {

    private Object source;
    private Object currentValue;

    public ValueWithSource(Object source, Object  curentValue){
      this.currentValue = curentValue;
      this.source =  source;
    }

    public String getSourceAsString(){
      return (String)source;
    }

    public String getCurrentValueAsString(){
      return (String)currentValue;
    }

    public Long getCurrentValueAsLong(){
      return (long) currentValue;
    }

    public <T> T getSourceAsClassType(Class<T> t){
      return t.cast(source);
    }
  }
}
