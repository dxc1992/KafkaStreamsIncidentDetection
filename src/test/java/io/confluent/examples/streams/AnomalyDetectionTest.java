/*
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

import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link WordCountLambdaExample}, using TopologyTestDriver.
 *
 * See {@link WordCountLambdaExample} for further documentation.
 */
public class AnomalyDetectionTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private StringSerializer stringSerializer = new StringSerializer();
  private StringDeserializer stringDeserializer = new StringDeserializer();
  private LongDeserializer longDeserializer = new LongDeserializer();

  @Before
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    String inputTopicStr = "test-input-topic";
    String outputTopicStr = "test-output-topic";
    //Create Actual Stream Processing pipeline
    AnomalyDetection.createAnomalyDetectionStream(builder, inputTopicStr, outputTopicStr );
    testDriver = new TopologyTestDriver(builder.build(), WordCountLambdaExample.getStreamsConfiguration("localhost:9092"));
    inputTopic = testDriver.createInputTopic(inputTopicStr,
                                             stringSerializer,
                                             stringSerializer);
    outputTopic = testDriver.createOutputTopic(outputTopicStr,
                                               stringDeserializer,
                                               stringDeserializer);
  }

  @After
  public void tearDown() {
    try {
      testDriver.close();
    } catch (final RuntimeException e) {
      // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
      // Logged stacktrace cannot be avoided
      System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
    }
  }


  /**
   *  Test Word count of sentence list.
   */
  @Test
  public void thresholdBasedAnomalyDetection() {
    String largeErrorBatch = "";
    String expectedOutputMessage = "";
    try {
      FileInputStream fis = new FileInputStream("src/test/resources/errorBatch.txt");
      largeErrorBatch = IOUtils.toString(fis, "UTF-8");
      FileInputStream fis1 = new FileInputStream("src/test/resources/expectedOutputMessageAnomalyDetection.txt");
      expectedOutputMessage = IOUtils.toString(fis1, "UTF-8");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final List<String> inputValues = Arrays.asList(
        "[2022-05-14 15:18:41,232] INFO [main] topology-test-driver Closing record collector clean (org.apache.kafka.streams.processor.internals.RecordCollectorImpl)\n" +
                "[2022-05-14 15:18:41,232] INFO [main] stream-thread [main] task [0_0] Closed clean (org.apache.kafka.streams.processor.internals.StreamTask)\n" +
                "[2022-05-14 15:18:41,232] INFO [main] stream-thread [main] Deleting task directory 0_0 for 0_0 as user calling cleanup. (org.apache.kafka.streams.processor.internals.StateDirectory)",
        largeErrorBatch
    );


    inputTopic.pipeValueList(inputValues, Instant.ofEpochMilli(1L), Duration.ofMillis(100L));
    assertThat(outputTopic.readValuesToList().get(0), equalTo(expectedOutputMessage));
  }

}
