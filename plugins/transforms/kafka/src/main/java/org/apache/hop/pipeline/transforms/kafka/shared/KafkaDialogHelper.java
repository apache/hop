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

import org.apache.commons.lang.StringUtils;
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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.TableItem;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaDialogHelper {
  private IVariables variables;
  private KafkaFactory kafkaFactory;
  private ComboVar wTopic;
  private TextVar wBootstrapServers;
  private TableView optionsTable;
  private TransformMeta parentMeta;

  public KafkaDialogHelper( IVariables variables, ComboVar wTopic, TextVar wBootstrapServers, KafkaFactory kafkaFactory,
                            TableView optionsTable, TransformMeta parentMeta ) {
    this.variables = variables;
    this.wTopic = wTopic;
    this.wBootstrapServers = wBootstrapServers;
    this.kafkaFactory = kafkaFactory;
    this.optionsTable = optionsTable;
    this.parentMeta = parentMeta;
  }

  public void clusterNameChanged( Event event ) {
    String current = wTopic.getText();
    if ( StringUtils.isEmpty( wBootstrapServers.getText() ) ) {
      return;
    }
    String directBootstrapServers = wBootstrapServers == null ? "" : wBootstrapServers.getText();
    Map<String, String> config = getConfig( optionsTable );
    CompletableFuture
      .supplyAsync( () -> listTopics( directBootstrapServers, config ) )
      .thenAccept( ( topicMap ) -> Display.getDefault().syncExec( () -> populateTopics( topicMap, current ) ) );
  }

  private void populateTopics( Map<String, List<PartitionInfo>> topicMap, String current ) {
    if ( !wTopic.getCComboWidget().isDisposed() ) {
      wTopic.getCComboWidget().removeAll();
    }
    topicMap.keySet().stream()
      .filter( key -> !"__consumer_offsets".equals( key ) ).sorted().forEach( key -> {
        if ( !wTopic.isDisposed() ) {
          wTopic.add( key );
        }
      } );
    if ( !wTopic.getCComboWidget().isDisposed() ) {
      wTopic.getCComboWidget().setText( current );
    }
  }

  private Map<String, List<PartitionInfo>> listTopics(
    final String directBootstrapServers, Map<String, String> config ) {
    Consumer kafkaConsumer = null;
    try {
      KafkaConsumerInputMeta localMeta = new KafkaConsumerInputMeta();
      localMeta.setDirectBootstrapServers( directBootstrapServers );
      localMeta.setConfig( config );
      localMeta.setParentTransformMeta( parentMeta );
      kafkaConsumer = kafkaFactory.consumer( localMeta, variables::resolve );
      Map<String, List<PartitionInfo>> topicMap = kafkaConsumer.listTopics();
      return topicMap;
    } catch ( Exception e ) {
      e.printStackTrace();
      return Collections.emptyMap();
    } finally {
      if ( kafkaConsumer != null ) {
        kafkaConsumer.close();
      }
    }
  }

  public static void populateFieldsList( IVariables variables, PipelineMeta pipelineMeta, ComboVar comboVar, String TransformName ) {
    String current = comboVar.getText();
    comboVar.getCComboWidget().removeAll();
    comboVar.setText( current );
    try {
      IRowMeta rmi = pipelineMeta.getPrevTransformFields( variables, TransformName );
      for ( int i = 0; i < rmi.size(); i++ ) {
        IValueMeta vmb = rmi.getValueMeta( i );
        comboVar.add( vmb.getName() );
      }
    } catch ( HopTransformException ex ) {
      // do nothing
      LogChannel.UI.logError( "Error getting fields", ex );
    }
  }

  public static List<String> getConsumerConfigOptionNames() {
    List<String> optionNames = getConfigOptionNames( ConsumerConfig.class );
    Stream.of( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfig.GROUP_ID_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ).forEach( optionNames::remove );
    return optionNames;
  }

  public static List<String> getProducerConfigOptionNames() {
    List<String> optionNames = getConfigOptionNames( ProducerConfig.class );
    Stream.of( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConfig.CLIENT_ID_CONFIG,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG )
        .forEach( optionNames::remove );
    return optionNames;
  }

  public static List<String> getConsumerAdvancedConfigOptionNames() {
    return Arrays.asList( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG );
  }

  public static List<String> getProducerAdvancedConfigOptionNames() {
    return Arrays.asList( ProducerConfig.COMPRESSION_TYPE_CONFIG,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG );
  }

  private static List<String> getConfigOptionNames( Class cl ) {
    return getStaticField( cl, "CONFIG" ).map( config ->
        ( (ConfigDef) config ).configKeys().keySet().stream().sorted().collect( Collectors.toList() )
    ).orElse( new ArrayList<>() );
  }

  private static Optional<Object> getStaticField( Class cl, String fieldName ) {
    Field field = null;
    boolean isAccessible = false;
    try {
      field = cl.getDeclaredField( fieldName );
      isAccessible = field.isAccessible();
      field.setAccessible( true );
      return Optional.ofNullable( field.get( null ) );
    } catch ( NoSuchFieldException | IllegalAccessException e ) {
      return Optional.empty();
    } finally {
      if ( field != null ) {
        field.setAccessible( isAccessible );
      }
    }
  }

  public static Map<String, String> getConfig( TableView optionsTable ) {
    int itemCount = optionsTable.getItemCount();
    Map<String, String> advancedConfig = new LinkedHashMap<>();

    for ( int rowIndex = 0; rowIndex < itemCount; rowIndex++ ) {
      TableItem row = optionsTable.getTable().getItem( rowIndex );
      String config = row.getText( 1 );
      String value = row.getText( 2 );
      if ( !StringUtils.isBlank( config ) && !advancedConfig.containsKey( config ) ) {
        advancedConfig.put( config, value );
      }
    }
    return advancedConfig;
  }
}
