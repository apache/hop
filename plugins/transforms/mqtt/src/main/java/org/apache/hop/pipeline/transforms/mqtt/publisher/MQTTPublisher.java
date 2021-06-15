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

package org.apache.hop.pipeline.transforms.mqtt.publisher;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mqtt.key.SSLSocketFactoryGenerator;
import org.eclipse.paho.client.mqttv3.*;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

public class MQTTPublisher extends BaseTransform<MQTTPublisherMeta, MQTTPublisherData>
        implements ITransform<MQTTPublisherMeta, MQTTPublisherData> {

  public MQTTPublisher(TransformMeta transformMeta, MQTTPublisherMeta meta, MQTTPublisherData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public void dispose( ) {
    shutdown( );
    super.dispose( );
  }

  protected void configureConnection( MQTTPublisherMeta meta, MQTTPublisherData data ) throws HopException {
    if ( data.client == null ) {
      String broker = resolve( meta.getBroker() );
      if ( Utils.isEmpty( broker ) ) {
        throw new HopException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.NoBrokerURL" ) );
      }
      String clientId = resolve( meta.getClientId() );
      if ( Utils.isEmpty( clientId ) ) {
        throw new HopException( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.NoClientID" ) );
      }

      try {
        data.client = new MqttClient( broker, clientId );

        MqttConnectOptions connectOptions = new MqttConnectOptions();
        if ( meta.isRequiresAuth() ) {
          connectOptions.setUserName( resolve( meta.getUsername() ) );
          connectOptions.setPassword( resolve( meta.getPassword() ).toCharArray() );
        }
        if ( broker.startsWith( "ssl:" ) || broker.startsWith( "wss:" ) ) {
          connectOptions.setSocketFactory( SSLSocketFactoryGenerator
              .getSocketFactory( resolve( meta.getSSLCaFile() ),
                  resolve( meta.getSSLCertFile() ), resolve( meta.getSSLKeyFile() ),
                  resolve( meta.getSSLKeyFilePass() ) ) );
        }
        connectOptions.setCleanSession( true );

        String timeout = resolve( meta.getTimeout() );
        try {
          connectOptions.setConnectionTimeout( Integer.parseInt( timeout ) );
        } catch ( NumberFormatException e ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongTimeoutValue.Message", timeout ), e );
        }

        logBasic( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.CreateMQTTClient.Message", broker, clientId ) );
        data.client.connect( connectOptions );

      } catch ( Exception e ) {
        throw new HopException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorCreateMQTTClient.Message", broker ),
            e );
      }
    }
  }

  public boolean processRow(  ) throws HopException {
    Object[] r = getRow();
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    IRowMeta inputRowMeta = getInputRowMeta();

    if ( first ) {
      first = false;

      // Initialize MQTT m_client:
      configureConnection( meta, data );

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      String inputField = resolve( meta.getField() );

      int numErrors = 0;
      if ( Utils.isEmpty( inputField ) ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.FieldNameIsNull" ) );
        numErrors++;
      }
      data.inputFieldNr = inputRowMeta.indexOfValue( inputField );
      if ( data.inputFieldNr < 0 ) {
        logError( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.CouldntFindField", inputField ) );
        numErrors++;
      }

      if ( numErrors > 0 ) {
        setErrors( numErrors );
        stopAll();
        return false;
      }
      data.inputFieldMeta = inputRowMeta.getValueMeta( data.inputFieldNr);
      data.topic = resolve( meta.getTopic() );
      if ( meta.getTopicIsFromField() ) {
        data.topicFromFieldIndex = inputRowMeta.indexOfValue( data.topic);
        if ( data.topicFromFieldIndex < 0 ) {
          throw new HopException(
              "Incoming stream does not seem to contain the topic field '" + data.topic + "'" );
        }

        if ( inputRowMeta.getValueMeta( data.topicFromFieldIndex).getType() != IValueMeta.TYPE_STRING ) {
          throw new HopException( "Incoming stream field to use for setting the topic must be of type string" );
        }
      }

      String qosValue = resolve( meta.getQoS() );
      try {
        data.qos = Integer.parseInt( qosValue );
        if ( data.qos < 0 || data.qos > 2 ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongQOSValue.Message", qosValue ) );
        }
      } catch ( NumberFormatException e ) {
        throw new HopException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongQOSValue.Message", qosValue ), e );
      }
    }

    try {
      if ( !isStopped() ) {
        Object rawMessage = r[data.inputFieldNr];
        byte[] message = messageToBytes( rawMessage, data.inputFieldMeta);
        if ( message == null ) {
          logDetailed( "Incoming message value is null/empty - skipping" );
          return true;
        }

        if ( meta.getTopicIsFromField() ) {
          if ( r[data.topicFromFieldIndex] == null || Utils.isEmpty( r[data.topicFromFieldIndex].toString() ) ) {
            // TODO add a default topic option, and then only skip if the default is null
            logDetailed( "Incoming topic value is null/empty - skipping message: " + rawMessage );
            return true;
          }
          data.topic = r[data.topicFromFieldIndex].toString();
        }

        MqttMessage mqttMessage = new MqttMessage( message );
        mqttMessage.setQos( data.qos);

        logBasic( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.SendingData", data.topic,
            Integer.toString( data.qos) ) );
        if ( isRowLevel() ) {
          logRowlevel( data.inputFieldMeta.getString( r[data.inputFieldNr] ) );
        }
        try {
          data.client.publish( data.topic, mqttMessage );
        } catch ( MqttException e ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorPublishing.Message" ), e );
        }
      }
    } catch ( HopException e ) {
      if ( !getTransformMeta().isDoingErrorHandling() ) {
        logError(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorInStepRunning", e.getMessage() ) );
        setErrors( 1 );
        stopAll();
        setOutputDone();
        return false;
      }
      putError( getInputRowMeta(), r, 1, e.toString(), null, getTransformName() );
    }
    return true;
  }

  protected void shutdown(  ) {

    if ( data.client != null ) {
      try {
        if ( data.client.isConnected() ) {
          data.client.disconnect();
        }
        data.client.close();
        data.client = null;
      } catch ( MqttException e ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorClosingMQTTClient.Message" ), e );
      }
    }
  }

  public void stopRunning( ) throws HopException {
    shutdown( );
    super.stopRunning( );
  }

  protected byte[] messageToBytes( Object message, IValueMeta messageValueMeta ) throws HopValueException {
    if ( message == null || Utils.isEmpty( message.toString() ) ) {
      return null;
    }

    byte[] result = null;
    try {
      ByteBuffer buff = null;
      switch ( messageValueMeta.getType() ) {
        case IValueMeta.TYPE_STRING:
          result = message.toString().getBytes( "UTF-8" );
          break;
        case IValueMeta.TYPE_INTEGER:
        case IValueMeta.TYPE_DATE: // send the date as a long (milliseconds) value
          buff = ByteBuffer.allocate( 8 );
          buff.putLong( messageValueMeta.getInteger( message ) );
          result = buff.array();
          break;
        case IValueMeta.TYPE_NUMBER:
          buff = ByteBuffer.allocate( 8 );
          buff.putDouble( messageValueMeta.getNumber( message ) );
          result = buff.array();
          break;
        case IValueMeta.TYPE_TIMESTAMP:
          buff = ByteBuffer.allocate( 12 );
          Timestamp ts = (Timestamp) message;
          buff.putLong( ts.getTime() );
          buff.putInt( ts.getNanos() );
          result = buff.array();
          break;
        case IValueMeta.TYPE_BINARY:
          result = messageValueMeta.getBinary( message );
          break;
        case IValueMeta.TYPE_BOOLEAN:
          result = new byte[1];
          if ( messageValueMeta.getBoolean( message ) ) {
            result[0] = 1;
          }
          break;
        case IValueMeta.TYPE_SERIALIZABLE:
          if ( !( message instanceof Serializable ) ) {
            throw new HopValueException( "Message value is not serializable!" );
          }
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream( bos );
          oos.writeObject( message );
          oos.flush();
          result = bos.toByteArray();
          break;
      }
    } catch ( Exception ex ) {
      throw new HopValueException( ex );
    }

    return result;
  }
}
