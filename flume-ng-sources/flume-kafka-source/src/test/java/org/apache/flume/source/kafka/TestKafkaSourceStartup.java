/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.source.kafka;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import kafka.common.TopicExistsException;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.*;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.flume.source.kafka.KafkaSourceConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestKafkaSourceStartup {
  private static final Logger log = LoggerFactory.getLogger(TestKafkaSourceStartup.class);

  private KafkaSource kafkaSource;
  private KafkaSourceEmbeddedKafka kafkaServer;
  private Context context;
  private List<Event> events;

  private final Set<String> usedTopics = new HashSet<String>();
  private String topic0 = "test1";
  private String topic1 = "topic1";

  public void setup() throws Exception {
  }

  ChannelProcessor createGoodChannel() {

    ChannelProcessor channelProcessor = mock(ChannelProcessor.class);

    events = Lists.newArrayList();

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        events.addAll((List<Event>)invocation.getArguments()[0]);
        return null;
      }
    }).when(channelProcessor).processEventBatch(any(List.class));

    return channelProcessor;
  }


  private Context prepareDefaultContext(String groupId) {
    Context context = new Context();
    context.put(BOOTSTRAP_SERVERS, kafkaServer.getBootstrapServers());
    context.put(KAFKA_CONSUMER_PREFIX + "group.id", groupId);
    return context;
  }

  public void tearDown() throws Exception {
    kafkaSource.stop();
    kafkaServer.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProcessItNotEmptyBatch() throws Exception {
    kafkaSource = new KafkaSource();
    kafkaServer = new KafkaSourceEmbeddedKafka(null, false);
    context = prepareDefaultContext("flume-group");
    kafkaSource.setChannelProcessor(createGoodChannel());

    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"2");
    kafkaSource.configure(context);
    kafkaSource.start();
    kafkaServer.start();

    Thread.sleep(500L);

    kafkaServer.createTopic(topic0, 1);

    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");
    kafkaServer.produce(topic0, "", "foo, bar");

    Thread.sleep(500L);

    Status status = kafkaSource.process();
    assertEquals(Status.READY, status);
    Assert.assertEquals("hello, world", new String(events.get(0).getBody(),
            Charsets.UTF_8));
    Assert.assertEquals("foo, bar", new String(events.get(1).getBody(),
            Charsets.UTF_8));

  }

}
