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

package org.apache.hop.pipeline.transforms.kafka.producer;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.kafka.consumer.KafkaConsumerField;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaFactory;
import org.apache.kafka.clients.producer.ProducerRecord;


public class KafkaProducerOutput extends BaseTransform<KafkaProducerOutputMeta, KafkaProducerOutputData>
                                 implements ITransform<KafkaProducerOutputMeta, KafkaProducerOutputData> {

  private static final Class<?> PKG = KafkaProducerOutputMeta.class; // For Translator

  private KafkaFactory kafkaFactory;

  public KafkaProducerOutput( TransformMeta transformMeta,
                              KafkaProducerOutputMeta meta,
                              KafkaProducerOutputData data, int copyNr,
                              PipelineMeta pipelineMeta, Pipeline trans ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, trans );
    setKafkaFactory( KafkaFactory.defaultFactory() );
  }

  void setKafkaFactory( KafkaFactory factory ) {
    this.kafkaFactory = factory;
  }


  @Override public boolean processRow() throws HopException {
    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      if ( data.kafkaProducer != null ) {
        data.kafkaProducer.close();
      }
      return false;
    }
    if ( first ) {
      data.keyFieldIndex = getInputRowMeta().indexOfValue( resolve( meta.getKeyField() ) );
      data.messageFieldIndex = getInputRowMeta().indexOfValue( resolve( meta.getMessageField() ) );
      IValueMeta keyValueMeta = getInputRowMeta().getValueMeta( data.keyFieldIndex );
      IValueMeta msgValueMeta = getInputRowMeta().getValueMeta( data.messageFieldIndex );

      data.kafkaProducer = kafkaFactory.producer( meta, this::resolve,
        KafkaConsumerField.Type.fromValueMeta( keyValueMeta ),
        KafkaConsumerField.Type.fromValueMeta( msgValueMeta ) );

      data.isOpen = true;

      first = false;
    }

    if ( !data.isOpen ) {
      return false;
    }
    ProducerRecord<Object, Object> producerRecord;
    // allow for null keys
    if ( data.keyFieldIndex < 0 || r[ data.keyFieldIndex ] == null || StringUtils.isEmpty( r[ data.keyFieldIndex ].toString() ) ) {
      producerRecord = new ProducerRecord<>( resolve( meta.getTopic() ), r[ data.messageFieldIndex ] );
    } else {
      producerRecord = new ProducerRecord<>( resolve( meta.getTopic() ), r[ data.keyFieldIndex ],
        r[ data.messageFieldIndex ] );
    }

    data.kafkaProducer.send( producerRecord );
    incrementLinesOutput();

    putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) && log.isBasic() ) {
      logBasic( BaseMessages.getString( PKG, "KafkaConsumerOutput.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  @Override
  public void stopRunning() {
    if ( data.kafkaProducer != null && data.isOpen ) {
      data.isOpen = false;
      data.kafkaProducer.flush();
      data.kafkaProducer.close();
    }
  }
}
