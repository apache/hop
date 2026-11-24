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
package org.apache.hop.pipeline.transforms.kafka.shared;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.lineage.LineageDatasetIoEmitter;
import org.apache.hop.lineage.LineageSinkRoutes;
import org.apache.hop.lineage.model.DatasetIoOperation;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.kafka.clients.CommonClientConfigs;

/**
 * The topics a Kafka transform reads or writes, for lineage. A topic is named as the OpenLineage
 * naming spec and DataHub's Kafka source name it: the namespace {@code kafka://<host>:<port>} of
 * the first bootstrap server, and the topic.
 *
 * <p>Whether lineage is on for the run is decided once, when this is created: with lineage off, a
 * row costs one field check. A producer reports a topic from Kafka's send callback, so {@link
 * #topic} can be called from Kafka's I/O thread.
 */
public final class KafkaLineage {

  /** The most distinct topics a transform reports, for a producer that takes them from a field. */
  public static final int MAX_TOPICS = 1000;

  private final ITransform transform;
  private final DatasetIoOperation operation;
  private final String namespace;
  private final boolean enabled;
  private final Set<String> reported = ConcurrentHashMap.newKeySet();

  public KafkaLineage(ITransform transform, DatasetIoOperation operation, String bootstrapServers) {
    this.transform = transform;
    this.operation = operation;
    this.namespace = namespace(bootstrapServers);
    this.enabled = namespace != null && lineageEnabled(transform);
  }

  private static boolean lineageEnabled(ITransform transform) {
    try {
      return LineageSinkRoutes.isEnabled(transform);
    } catch (Exception | LinkageError e) {
      // Lineage must not stop the run
      return false;
    }
  }

  /**
   * The bootstrap servers the Kafka client connects to: a {@code bootstrap.servers} option replaces
   * the transform's own value when it is set, as it does in the producer's configuration.
   */
  public static String bootstrapServers(
      String directBootstrapServers, List<KafkaOption> options, UnaryOperator<String> resolve) {
    String servers = resolve.apply(directBootstrapServers);
    if (options != null) {
      for (KafkaOption option : options) {
        if (option != null
            && CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG.equals(
                StringUtils.trim(option.getProperty()))) {
          servers = resolve.apply(option.getValue());
        }
      }
    }
    return servers;
  }

  /**
   * {@code kafka://<host>:<port>} for the first of the bootstrap servers, or null without one.
   * Kafka's default port is assumed when the server has none.
   */
  public static String namespace(String bootstrapServers) {
    if (StringUtils.isBlank(bootstrapServers)) {
      return null;
    }
    String server = bootstrapServers.split(",")[0].trim();
    int scheme = server.indexOf("://");
    if (scheme >= 0) {
      // A listener such as SASL_SSL://broker:9093
      server = server.substring(scheme + 3);
    }
    server = StringUtils.removeEnd(server, "/");
    if (server.isEmpty()) {
      return null;
    }
    if (!server.contains(":")) {
      server = server + ":9092";
    }
    return "kafka://" + server;
  }

  /**
   * Whether a topic is still to be reported: lineage is on for the run, the topic was not reported
   * yet and the cap is not reached.
   */
  public boolean wants(String topic) {
    return enabled
        && !StringUtils.isEmpty(topic)
        && !reported.contains(topic)
        && reported.size() < MAX_TOPICS;
  }

  /**
   * Reports a topic the first time it is read or written. Does nothing when lineage is off for the
   * run, and past {@link #MAX_TOPICS} distinct topics.
   */
  public void topic(String topic) {
    synchronized (reported) {
      if (!wants(topic)) {
        return;
      }
      reported.add(topic);
    }
    LineageDatasetIoEmitter.emitTransformDatasetIo(
        transform, operation, namespace, topic, true, null);
  }

  /** The topics reported so far. */
  Set<String> reported() {
    return reported;
  }
}
