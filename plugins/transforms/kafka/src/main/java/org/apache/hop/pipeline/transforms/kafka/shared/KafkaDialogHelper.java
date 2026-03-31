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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.kafka.consumer.KafkaConsumerInputMeta;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.SslConfigs;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.TableItem;

public class KafkaDialogHelper {
  private final IVariables variables;
  private final KafkaFactory kafkaFactory;
  private final ComboVar wTopic;
  private final TextVar wBootstrapServers;
  private final TableView optionsTable;
  private final TransformMeta parentMeta;

  public KafkaDialogHelper(
      IVariables variables,
      ComboVar wTopic,
      TextVar wBootstrapServers,
      KafkaFactory kafkaFactory,
      TableView optionsTable,
      TransformMeta parentMeta) {
    this.variables = variables;
    this.wTopic = wTopic;
    this.wBootstrapServers = wBootstrapServers;
    this.kafkaFactory = kafkaFactory;
    this.optionsTable = optionsTable;
    this.parentMeta = parentMeta;
  }

  public void clusterNameChanged(Event event) {
    String current = wTopic.getText();
    if (StringUtils.isEmpty(wBootstrapServers.getText())) {
      return;
    }
    String directBootstrapServers = wBootstrapServers.getText();
    List<KafkaOption> options = getConfig(optionsTable);
    CompletableFuture.supplyAsync(() -> listTopics(directBootstrapServers, options))
        .thenAccept(
            topicMap ->
                HopGui.getInstance()
                    .getDisplay()
                    .syncExec(() -> populateTopics(topicMap, current)));
  }

  private void populateTopics(Map<String, List<PartitionInfo>> topicMap, String current) {
    if (!wTopic.getCComboWidget().isDisposed()) {
      wTopic.getCComboWidget().removeAll();
    }
    topicMap.keySet().stream()
        .filter(key -> !key.startsWith("_"))
        .sorted()
        .forEach(
            key -> {
              if (!wTopic.isDisposed()) {
                wTopic.add(key);
              }
            });
    if (!wTopic.getCComboWidget().isDisposed()) {
      wTopic.getCComboWidget().setText(current);
    }
  }

  private Map<String, List<PartitionInfo>> listTopics(
      final String directBootstrapServers, List<KafkaOption> options) {
    Consumer kafkaConsumer = null;
    try {
      KafkaConsumerInputMeta localMeta = new KafkaConsumerInputMeta();
      localMeta.setDirectBootstrapServers(directBootstrapServers);
      localMeta.setOptions(options);
      localMeta.setParentTransformMeta(parentMeta);
      kafkaConsumer = kafkaFactory.consumer(localMeta, variables::resolve);
      return kafkaConsumer.listTopics();
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error getting list of topics", e);
      return Collections.emptyMap();
    } finally {
      if (kafkaConsumer != null) {
        kafkaConsumer.close();
      }
    }
  }

  public static void populateFieldsList(
      IVariables variables, PipelineMeta pipelineMeta, ComboVar comboVar, String transformName) {
    String current = comboVar.getText();
    comboVar.getCComboWidget().removeAll();
    comboVar.setText(current);
    try {
      IRowMeta rmi = pipelineMeta.getPrevTransformFields(variables, transformName);
      for (int i = 0; i < rmi.size(); i++) {
        IValueMeta vmb = rmi.getValueMeta(i);
        comboVar.add(vmb.getName());
      }
    } catch (HopTransformException ex) {
      // do nothing
      LogChannel.UI.logError("Error getting fields", ex);
    }
  }

  public static List<String> getConsumerAdvancedConfigOptionNames() {
    return Arrays.asList(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG,
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
  }

  public static List<String> getProducerAdvancedConfigOptionNames() {
    return Arrays.asList(
        ProducerConfig.COMPRESSION_TYPE_CONFIG,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG,
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
  }

  public static List<KafkaOption> getConfig(TableView optionsTable) {
    List<KafkaOption> advancedConfig = new ArrayList<>();
    for (TableItem item : optionsTable.getNonEmptyItems()) {
      String config = item.getText(1);
      String value = item.getText(2);
      advancedConfig.add(new KafkaOption(config, value));
    }
    return advancedConfig;
  }
}
