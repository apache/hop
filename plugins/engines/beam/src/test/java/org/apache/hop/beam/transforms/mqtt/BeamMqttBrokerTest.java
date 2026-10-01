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

package org.apache.hop.beam.transforms.mqtt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Wire-level integration against a loopback-only MQTT 3.1 fixture, not a live service. */
class BeamMqttBrokerTest {
  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  @Timeout(45)
  void beamSourceReadsBinaryPayloadAndStopsAtRecordLimit() throws Exception {
    byte[] payload = {0, -1, 42, 9};
    try (var broker = new Broker(payload)) {
      var meta = new BeamMqttInputMeta();
      meta.setServerUri(broker.uri());
      meta.setTopic("sensors/#");
      meta.setPayloadType("Binary");
      meta.setMaxNumRecords("1");
      meta.setMaxReadTime("10");
      Pipeline p = Pipeline.create();
      var rows = p.apply(meta.buildInputTransform(new Variables(), "source"));
      PAssert.that(rows)
          .satisfies(
              values -> {
                int count = 0;
                for (HopRow row : values) {
                  assertArrayEquals(payload, (byte[]) row.getRow()[0]);
                  count++;
                }
                assertEquals(1, count);
                return null;
              });
      p.run().waitUntilFinish();
      assertEquals("sensors/#", broker.subscriptions.poll(5, TimeUnit.SECONDS));
      assertTrue(broker.acks.poll(5, TimeUnit.SECONDS));
      broker.assertHealthy();
    }
  }

  @Test
  @Timeout(45)
  void beamSinkPublishesUtf8RetainedQosOneWithResolvedAuthentication() throws Exception {
    try (var broker = new Broker(null)) {
      var vars = new Variables();
      vars.setVariable("BROKER", broker.uri());
      vars.setVariable("TOPIC", "sensors/data");
      vars.setVariable("FIELD", "body");
      vars.setVariable("USER", "test-user");
      vars.setVariable(
          "SECRET",
          org.apache.hop.core.encryption.Encr.encryptPasswordIfNotUsingVariables("test-password"));
      var meta = new BeamMqttOutputMeta();
      meta.setServerUri("${BROKER}");
      meta.setTopic("${TOPIC}");
      meta.setPayloadField("${FIELD}");
      meta.setUsername("${USER}");
      meta.setPassword("${SECRET}");
      meta.setClientId("hop-write");
      meta.setRetained(true);
      var rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaString("body"));
      Pipeline p = Pipeline.create();
      p.apply(
              Create.of(new HopRow(new Object[] {"héllo 😀"}), new HopRow(new Object[] {""}))
                  .withCoder(new HopRowCoder()))
          .apply(meta.buildOutputTransform(vars, "sink", rowMeta));
      p.run().waitUntilFinish();
      var one = broker.messages.poll(5, TimeUnit.SECONDS);
      var two = broker.messages.poll(5, TimeUnit.SECONDS);
      assertNotNull(one);
      assertNotNull(two);
      for (var message : List.of(one, two)) {
        assertEquals("sensors/data", message.topic());
        assertTrue(message.retained());
        assertEquals(1, message.qos());
      }
      assertEquals(
          Set.of("héllo 😀", ""),
          Set.of(
              new String(one.payload(), StandardCharsets.UTF_8),
              new String(two.payload(), StandardCharsets.UTF_8)));
      var credentials = broker.credentials.poll(5, TimeUnit.SECONDS);
      assertNotNull(credentials);
      assertEquals("test-user", credentials.username());
      assertEquals("test-password", credentials.password());
      assertTrue(credentials.clientId().startsWith("hop-write-"));
      broker.assertHealthy();
    }
  }

  @Test
  @Timeout(45)
  void beamSinkPublishesLazyAndIndexedBytesThroughTheOutputHandler() throws Exception {
    byte[] latin1 = "caf\u00e9".getBytes(StandardCharsets.ISO_8859_1);
    RowMeta lazyMeta = new RowMeta();
    ValueMetaString lazy = new ValueMetaString("body");
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    ValueMetaString storage = new ValueMetaString("body");
    storage.setStringEncoding(StandardCharsets.ISO_8859_1.name());
    lazy.setStorageMetadata(storage);
    lazyMeta.addValueMeta(lazy);

    RowMeta indexedText = new RowMeta();
    ValueMetaString indexed = new ValueMetaString("body");
    indexed.setStorageType(IValueMeta.STORAGE_TYPE_INDEXED);
    indexed.setIndex(new Object[] {"alpha", "bravo"});
    indexedText.addValueMeta(indexed);

    RowMeta indexedBytes = new RowMeta();
    ValueMetaBinary binary = new ValueMetaBinary("body");
    binary.setStorageType(IValueMeta.STORAGE_TYPE_INDEXED);
    binary.setIndex(new Object[] {new byte[] {1}, new byte[] {9, -1}});
    indexedBytes.addValueMeta(binary);

    try (var broker = new Broker(null)) {
      publish(broker, lazyMeta, "String", new Object[] {latin1});
      publish(broker, indexedText, "String", new Object[] {1});
      publish(broker, indexedBytes, "Binary", new Object[] {1});
      assertArrayEquals(
          "caf\u00e9".getBytes(StandardCharsets.UTF_8), nextPayload(broker).payload());
      assertArrayEquals("bravo".getBytes(StandardCharsets.UTF_8), nextPayload(broker).payload());
      assertArrayEquals(new byte[] {9, -1}, nextPayload(broker).payload());
      broker.assertHealthy();
    }
  }

  @Test
  @Timeout(45)
  void beamSinkFailsWhenTheBrokerRefusesTheConnection() throws Exception {
    try (var broker = new Broker(null, Failure.REJECT_CONNECT)) {
      RuntimeException error = assertThrows(RuntimeException.class, () -> publishOne(broker));
      assertNotNull(error);
      assertEquals(0, broker.messages.size());
    }
  }

  @Test
  @Timeout(45)
  void beamSinkFailsWhenTheBrokerAcksAnUnknownPacketId() throws Exception {
    try (var broker = new Broker(null, Failure.ACK_UNKNOWN_ID)) {
      assertThrows(RuntimeException.class, () -> publishOne(broker));
      // The payload was received. PUBACK for a different packet id is fatal in mqtt-client
      // 1.15, so the pipeline fails instead of reporting success.
      assertEquals(1, broker.messages.size());
    }
  }

  @Test
  @Timeout(45)
  void beamSinkResendsAnUnacknowledgedPublishAfterReconnect() throws Exception {
    try (var broker = new Broker(null, Failure.RESEND_AFTER_DROP)) {
      var result = publishOne(broker);
      assertEquals(2, broker.messages.size());
      Published first = broker.messages.poll();
      Published second = broker.messages.poll();
      assertNotNull(first);
      assertNotNull(second);
      assertArrayEquals(first.payload(), second.payload());
      assertEquals("sensors/data", first.topic());
      assertEquals("sensors/data", second.topic());
      assertEquals(1, metric(result, org.apache.hop.pipeline.Pipeline.METRIC_NAME_READ));
      assertEquals(1, metric(result, org.apache.hop.pipeline.Pipeline.METRIC_NAME_OUTPUT));
      assertEquals(0, metric(result, org.apache.hop.pipeline.Pipeline.METRIC_NAME_WRITTEN));
    }
  }

  private static org.apache.beam.sdk.PipelineResult publishOne(Broker broker) throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("body"));
    return publish(broker, rowMeta, "String", new Object[] {"payload"});
  }

  private static org.apache.beam.sdk.PipelineResult publish(
      Broker broker, RowMeta rowMeta, String payloadType, Object[] row) throws Exception {
    var meta = new BeamMqttOutputMeta();
    meta.setServerUri(broker.uri());
    meta.setTopic("sensors/data");
    meta.setPayloadField("body");
    meta.setPayloadType(payloadType);
    Pipeline p = Pipeline.create();
    p.apply(Create.of(new HopRow(row)).withCoder(new HopRowCoder()))
        .apply(meta.buildOutputTransform(new Variables(), "sink", rowMeta));
    var result = p.run();
    result.waitUntilFinish();
    return result;
  }

  /** Absent counters count as zero. MqttIO does not increment the written counter. */
  private static long metric(org.apache.beam.sdk.PipelineResult result, String name) {
    var metrics =
        result
            .metrics()
            .queryMetrics(
                org.apache.beam.sdk.metrics.MetricsFilter.builder()
                    .addNameFilter(org.apache.beam.sdk.metrics.MetricNameFilter.named(name, "sink"))
                    .build());
    long total = 0;
    for (var counter : metrics.getCounters()) total += counter.getAttempted();
    return total;
  }

  private static Published nextPayload(Broker broker) throws InterruptedException {
    Published message = broker.messages.poll(5, TimeUnit.SECONDS);
    assertNotNull(message);
    return message;
  }

  private enum Failure {
    NONE,
    REJECT_CONNECT,
    /** Acknowledge a packet id the client did not publish. mqtt-client treats that as fatal. */
    ACK_UNKNOWN_ID,
    /** Drop the first publish, accept the reconnect, and acknowledge the resent copy. */
    RESEND_AFTER_DROP
  }

  private record Published(String topic, byte[] payload, boolean retained, int qos) {}

  private record Credentials(String clientId, String username, String password) {}

  /** Only the packets needed by these tests: CONNECT, SUBSCRIBE, QoS 1 PUBLISH, ACK and PING. */
  private static class Broker implements AutoCloseable {
    final ServerSocket server;
    final byte[] sourcePayload;
    final Failure failure;
    final ExecutorService executor =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "mqtt-test-broker");
              t.setDaemon(true);
              return t;
            });
    final List<Socket> clients = new CopyOnWriteArrayList<>();
    final BlockingQueue<Published> messages = new LinkedBlockingQueue<>();
    final BlockingQueue<Credentials> credentials = new LinkedBlockingQueue<>();
    final BlockingQueue<String> subscriptions = new LinkedBlockingQueue<>();
    final BlockingQueue<Boolean> acks = new LinkedBlockingQueue<>();
    final BlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
    volatile boolean closed;

    Broker(byte[] payload) throws IOException {
      this(payload, Failure.NONE);
    }

    Broker(byte[] payload, Failure failure) throws IOException {
      sourcePayload = payload;
      this.failure = failure;
      server = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
      executor.submit(
          () -> {
            while (!closed)
              try {
                Socket socket = server.accept();
                clients.add(socket);
                executor.submit(() -> serve(socket));
              } catch (IOException e) {
                if (!closed) errors.add(e);
              }
          });
    }

    String uri() {
      return "tcp://127.0.0.1:" + server.getLocalPort();
    }

    void assertHealthy() {
      assertTrue(errors.isEmpty(), () -> "Broker errors: " + errors);
    }

    void serve(Socket socket) {
      try (socket) {
        socket.setSoTimeout(20000);
        var input = socket.getInputStream();
        var output = socket.getOutputStream();
        while (!closed) {
          int header = input.read();
          if (header < 0) return;
          int size = 0, multiplier = 1, b;
          do {
            b = input.read();
            if (b < 0) throw new EOFException();
            size += (b & 127) * multiplier;
            multiplier *= 128;
          } while ((b & 128) != 0);
          byte[] bytes = input.readNBytes(size);
          if (bytes.length != size) throw new EOFException();
          var data = new DataInputStream(new ByteArrayInputStream(bytes));
          switch (header >> 4) {
            case 1 -> {
              text(data);
              data.readUnsignedByte();
              int flags = data.readUnsignedByte();
              data.readUnsignedShort();
              String id = text(data);
              if ((flags & 4) != 0) {
                text(data);
                text(data);
              }
              String user = (flags & 128) != 0 ? text(data) : null;
              String pass = (flags & 64) != 0 ? text(data) : null;
              credentials.add(new Credentials(id, user, pass));
              if (failure == Failure.REJECT_CONNECT) {
                // CONNACK return code 5. A rejected login fails the connect future.
                packet(output, 0x20, new byte[] {0, 5});
                return;
              }
              packet(output, 0x20, new byte[] {0, 0});
            }
            case 8 -> {
              int id = data.readUnsignedShort();
              String filter = text(data);
              data.readUnsignedByte();
              subscriptions.add(filter);
              packet(output, 0x90, new byte[] {(byte) (id >> 8), (byte) id, 1});
              if (sourcePayload != null) {
                var buffer = new ByteArrayOutputStream();
                var body = new DataOutputStream(buffer);
                text(body, "sensors/data");
                body.writeShort(42);
                body.write(sourcePayload);
                packet(output, 0x32, buffer.toByteArray());
              }
            }
            case 3 -> {
              String topic = text(data);
              int qos = (header >> 1) & 3;
              int id = qos > 0 ? data.readUnsignedShort() : 0;
              byte[] payload = data.readAllBytes();
              messages.add(new Published(topic, payload, (header & 1) != 0, qos));
              if (failure == Failure.RESEND_AFTER_DROP && messages.size() == 1) {
                socket.close();
                return;
              }
              if (qos == 1) {
                // mqtt-client 1.15 fails the publish future when PUBACK names an unknown id.
                int ackId = failure == Failure.ACK_UNKNOWN_ID ? (id == 0 ? 1 : 0) : id;
                packet(output, 0x40, new byte[] {(byte) (ackId >> 8), (byte) ackId});
              }
            }
            case 4 -> acks.add(true);
            case 12 -> packet(output, 0xd0, new byte[0]);
            case 14 -> {
              return;
            }
            default -> throw new IOException("Unexpected MQTT packet type: " + (header >> 4));
          }
        }
      } catch (IOException e) {
        if (!closed && !(e instanceof EOFException)) errors.add(e);
      }
    }

    private static String text(DataInputStream data) throws IOException {
      return new String(data.readNBytes(data.readUnsignedShort()), StandardCharsets.UTF_8);
    }

    private static void text(DataOutputStream data, String value) throws IOException {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      data.writeShort(bytes.length);
      data.write(bytes);
    }

    private static void packet(OutputStream output, int header, byte[] body) throws IOException {
      output.write(header);
      int length = body.length;
      do {
        int digit = length % 128;
        length /= 128;
        output.write(digit | (length > 0 ? 128 : 0));
      } while (length > 0);
      output.write(body);
      output.flush();
    }

    @Override
    public void close() throws IOException {
      closed = true;
      server.close();
      for (Socket socket : clients) socket.close();
      executor.shutdownNow();
    }
  }
}
