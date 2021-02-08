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

package org.apache.hop.pipeline.transforms.mqtt.subscriber;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mqtt.key.SSLSocketFactoryGenerator;
import org.apache.hop.pipeline.transforms.mqtt.publisher.MQTTPublisherMeta;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class MQTTSubscriber extends BaseTransform<MQTTSubscriberMeta, MQTTSubscriberData>
        implements ITransform<MQTTSubscriberMeta, MQTTSubscriberData> {

  protected boolean m_reconnectFailed;

  public MQTTSubscriber(TransformMeta transformMeta, MQTTSubscriberMeta meta, MQTTSubscriberData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow( ) throws HopException {

    if ( !isStopped() ) {

      if ( first ) {
        first = false;

        if ( data.executionDuration > 0 ) {
          data.startTime = new Date();
        }

        data.outputRowMeta = new RowMeta();

        meta.getFields( data.outputRowMeta, getTransformName(), null, null, variables, metadataProvider);
      }

      if ( m_reconnectFailed ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.ReconnectFailed" ) );
        setStopped( true );
        return false;
      }

      if ( data.executionDuration > 0 ) {
        if ( System.currentTimeMillis() - data.startTime.getTime()
            > data.executionDuration * 1000 ) {
          setOutputDone();
          return false;
        }
      }

      return true;
    } else {
      setStopped( true );
      return false;
    }
  }

  protected synchronized void shutdown( ) {
    if ( data.client != null ) {
      try {
        if ( data.client.isConnected() ) {
          logBasic( "Disconnecting from MQTT broker" );
          data.client.disconnect();
        }
        data.client.close();
        data.client = null;
      } catch ( MqttException e ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorClosingMQTTClient.Message" ), e );
      }
    }
  }

  public boolean init() {
    if ( super.init() ) {
      try {
        configureConnection();
        String runFor = meta.getExecuteForDuration();
        try {
          data.executionDuration = Long.parseLong( runFor );
        } catch ( NumberFormatException e ) {
          logError( e.getMessage(), e );
          return false;
        }
      } catch ( HopException e ) {
        logError( e.getMessage(), e );
        return false;
      }

      try {
        IValueMeta
            messageMeta =
            ValueMetaFactory.createValueMeta( "Message",
                ValueMetaFactory.getIdForValueMeta( meta.getMessageType() ) );
        if ( messageMeta.isSerializableType() && !meta.getAllowReadMessageOfTypeObject() ) {
          logError( BaseMessages
              .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.MessageTypeObjectButObjectNotAllowed" ) );
          return false;
        }
      } catch ( HopPluginException e ) {
        logError( e.getMessage(), e );
        return false;
      }

      return true;
    }

    return false;
  }

  public void dispose(  ) {
 
    shutdown( );
    super.dispose();
  }

  public void stopRunning() throws HopException {

    shutdown( );
    super.stopRunning();
  }

  protected void configureConnection() throws HopException {
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
      List<String> topics = meta.getTopics();
      if ( topics == null || topics.size() == 0 ) {
        throw new HopException( "No topic(s) to subscribe to provided" );
      }
      List<String> resolvedTopics = new ArrayList<>();
      for ( String topic : topics ) {
        resolvedTopics.add( resolve( topic ) );
      }

      String qosS = resolve( meta.getQoS() );
      int qos = 0;
      if ( !Utils.isEmpty( qosS ) ) {
        try {
          qos = Integer.parseInt( qosS );
        } catch ( NumberFormatException e ) {
          // quietly ignore
        }
      }
      int[] qoss = new int[resolvedTopics.size()];
      for ( int i = 0; i < qoss.length; i++ ) {
        qoss[i] = qos;
      }
      try {
        data.client = new MqttClient( broker, clientId );

        MqttConnectOptions connectOptions = new MqttConnectOptions();
        if ( meta.isRequiresAuth() ) {
          connectOptions.setUserName( resolve( meta.getUsername() ) );
          connectOptions.setPassword( resolve( meta.getPassword() ).toCharArray() );
        }
        if ( broker.startsWith( "ssl:" ) || broker.startsWith( "wss:" ) ) {
          connectOptions.setSocketFactory( SSLSocketFactoryGenerator.getSocketFactory( resolve( meta.getSSLCaFile() ),
                  resolve( meta.getSSLCertFile() ), resolve( meta.getSSLKeyFile() ),
                  resolve( meta.getSSLKeyFilePass() ) ) );
        }
        connectOptions.setCleanSession( true );

        String timeout = resolve( meta.getTimeout() );
        String keepAlive = resolve( meta.getKeepAliveInterval() );
        try {
          connectOptions.setConnectionTimeout( Integer.parseInt( timeout ) );
        } catch ( NumberFormatException e ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongTimeoutValue.Message", timeout ), e );
        }

        try {
          connectOptions.setKeepAliveInterval( Integer.parseInt( keepAlive ) );
        } catch ( NumberFormatException e ) {
          throw new HopException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongKeepAliveValue.Message", keepAlive ),
              e );
        }

        logBasic( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.CreateMQTTClient.Message", broker, clientId ) );

        data.client.setCallback( new SubscriberCallback( data, meta ) );
        data.client.connect( connectOptions );

        data.client.subscribe( resolvedTopics.toArray( new String[resolvedTopics.size()] ), qoss );
      } catch ( Exception e ) {
        throw new HopException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorCreateMQTTClient.Message", broker ),
            e );
      }
    }
  }

  protected class SubscriberCallback implements MqttCallback {

    protected MQTTSubscriberData m_data;
    protected MQTTSubscriberMeta m_meta;
    protected IValueMeta m_messageValueMeta;

    public SubscriberCallback( MQTTSubscriberData data, MQTTSubscriberMeta meta ) throws HopPluginException {
      m_data = data;
      m_meta = meta;

      m_messageValueMeta =
          ValueMetaFactory.createValueMeta( "Message", ValueMetaFactory.getIdForValueMeta( m_meta.getMessageType() ) );
    }

    @Override 
    public void connectionLost( Throwable throwable ) {
      // connection retry logic here
      shutdown( );
      logBasic( BaseMessages
          .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.LostConnectionToBroker", throwable.getMessage() ) );
      logBasic( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.AttemptingToReconnect" ) );
      try {
        configureConnection();
      } catch ( HopException e ) {
        logError( e.getMessage(), e );
        m_reconnectFailed = true;
      }
    }

    @Override
    public void messageArrived( String topic, MqttMessage mqttMessage ) throws Exception {
      Object[] outRow = RowDataUtil.allocateRowData( m_data.outputRowMeta.size() );
      outRow[0] = topic;
      Object converted = null;

      byte[] raw = mqttMessage.getPayload();
      ByteBuffer buff = null;
      switch ( m_messageValueMeta.getType() ) {
        case IValueMeta.TYPE_INTEGER:
          buff = ByteBuffer.wrap( raw );
          outRow[1] = raw.length == 4 ? (long) buff.getInt() : buff.getLong();
          break;
        case IValueMeta.TYPE_STRING:
        case IValueMeta.TYPE_NONE:
          outRow[1] = new String( raw );
          break;
        case IValueMeta.TYPE_NUMBER:
          buff = ByteBuffer.wrap( raw );
          outRow[1] = raw.length == 4 ? (double) buff.getFloat() : buff.getDouble();
          break;
        case IValueMeta.TYPE_DATE:
          buff = ByteBuffer.wrap( raw );
          outRow[1] = new Date( buff.getLong() );
          break;
        case IValueMeta.TYPE_BINARY:
          outRow[1] = raw;
          break;
        case IValueMeta.TYPE_BOOLEAN:
          outRow[1] = raw[0] > 0;
          break;
        case IValueMeta.TYPE_TIMESTAMP:
          buff = ByteBuffer.wrap( raw );
          long time = buff.getLong();
          int nanos = buff.getInt();
          Timestamp t = new Timestamp( time );
          t.setNanos( nanos );
          outRow[1] = t;
          break;
        case IValueMeta.TYPE_SERIALIZABLE:
          ObjectInputStream ois = new ObjectInputStream( new ByteArrayInputStream( raw ) );
          outRow[1] = ois.readObject();
          break;
        default:
          throw new HopException( "Unhandled type" );
      }
      putRow( m_data.outputRowMeta, outRow );
    }

    @Override
    public void deliveryComplete( IMqttDeliveryToken iMqttDeliveryToken ) {

    }
  }
}
