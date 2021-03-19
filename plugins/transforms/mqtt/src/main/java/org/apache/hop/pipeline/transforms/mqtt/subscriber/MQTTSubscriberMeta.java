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

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;


@Transform(
        id = "MQTTSubscriberMeta",
        image = "MQTTSubscriberIcon.svg",
        name = "i18n::MQTTSubscriber.Step.Name",
        description = "i18n::MQTTSubscriber.Step.Description",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
        documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/mqttsubscriber.html")
public class MQTTSubscriberMeta
        extends BaseTransformMeta
        implements ITransformMeta<MQTTSubscriber, MQTTSubscriberData> {
  public static Class<?> PKG = MQTTSubscriberMeta.class;

  protected String broker = "";

  protected List<String> topics = new ArrayList<>();

  protected String messageType = IValueMeta.getTypeDescription( IValueMeta.TYPE_STRING );

  private String clientId;
  private String timeout = "30"; // seconds according to the mqtt javadocs
  private String keepAliveInterval = "60"; // seconds according to the mqtt javadocs
  private String qos = "0";
  private boolean requiresAuth;
  private String username;
  private String password;
  private String sslCaFile;
  private String sslCertFile;
  private String sslKeyFile;
  private String sslKeyFilePass;

  /**
   * Whether to allow messages of type object to be deserialized off the wire
   */
  private boolean allowReadObjectMessageType;

  /**
   * Execute for x seconds (0 means indefinitely)
   */
  private String executeForDuration = "0";

  /**
   * @return Broker URL
   */
  public String getBroker() {
    return broker;
  }

  /**
   * @param broker Broker URL
   */
  public void setBroker( String broker ) {
    this.broker = broker;
  }

  public void setTopics( List<String> topics ) {
    this.topics = topics;
  }

  public List<String> getTopics() {
    return topics;
  }

  /**
   * @param type the Kettle type of the message being received
   */
  public void setMessageType( String type ) {
    messageType = type;
  }

  /**
   * @return the Kettle type of the message being received
   */
  public String getMessageType() {
    return messageType;
  }

  /**
   * @return Client ID
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * @param clientId Client ID
   */
  public void setClientId( String clientId ) {
    this.clientId = clientId;
  }

  /**
   * @return Connection m_timeout
   */
  public String getTimeout() {
    return timeout;
  }

  /**
   * @param timeout Connection m_timeout
   */
  public void setTimeout( String timeout ) {
    this.timeout = timeout;
  }

  /**
   * @param interval interval in seconds
   */
  public void setKeepAliveInterval( String interval ) {
    keepAliveInterval = interval;
  }

  /**
   * @return the keep alive interval (in seconds)
   */
  public String getKeepAliveInterval() {
    return keepAliveInterval;
  }

  /**
   * @return QoS to use
   */
  public String getQoS() {
    return qos;
  }

  /**
   * @param qos QoS to use
   */
  public void setQoS( String qos ) {
    this.qos = qos;
  }

  /**
   * @return Whether MQTT broker requires authentication
   */
  public boolean isRequiresAuth() {
    return requiresAuth;
  }

  /**
   * @param requiresAuth Whether MQTT broker requires authentication
   */
  public void setRequiresAuth( boolean requiresAuth ) {
    this.requiresAuth = requiresAuth;
  }

  /**
   * @return Username to MQTT broker
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username Username to MQTT broker
   */
  public void setUsername( String username ) {
    this.username = username;
  }

  /**
   * @return Password to MQTT broker
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password Password to MQTT broker
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return Server CA file
   */
  public String getSSLCaFile() {
    return sslCaFile;
  }

  /**
   * @param sslCaFile Server CA file
   */
  public void setSSLCaFile( String sslCaFile ) {
    this.sslCaFile = sslCaFile;
  }

  /**
   * @return Client certificate file
   */
  public String getSSLCertFile() {
    return sslCertFile;
  }

  /**
   * @param sslCertFile Client certificate file
   */
  public void setSSLCertFile( String sslCertFile ) {
    this.sslCertFile = sslCertFile;
  }

  /**
   * @return Client key file
   */
  public String getSSLKeyFile() {
    return sslKeyFile;
  }

  /**
   * @param sslKeyFile Client key file
   */
  public void setSSLKeyFile( String sslKeyFile ) {
    this.sslKeyFile = sslKeyFile;
  }

  /**
   * @return Client key file m_password
   */
  public String getSSLKeyFilePass() {
    return sslKeyFilePass;
  }

  /**
   * @param sslKeyFilePass Client key file m_password
   */
  public void setSSLKeyFilePass( String sslKeyFilePass ) {
    this.sslKeyFilePass = sslKeyFilePass;
  }

  /**
   * @param duration the duration (in seconds) to run for. 0 indicates run indefinitely
   */
  public void setExecuteForDuration( String duration ) {
    executeForDuration = duration;
  }

  /**
   * @return the number of seconds to run for (0 means run indefinitely)
   */
  public String getExecuteForDuration() {
    return executeForDuration;
  }

  /**
   * @param allow true to allow object messages to be deserialized off of the wire
   */
  public void setAllowReadMessageOfTypeObject( boolean allow ) {
    allowReadObjectMessageType = allow;
  }

  /**
   * @return true if deserializing object messages is ok
   */
  public boolean getAllowReadMessageOfTypeObject() {
    return allowReadObjectMessageType;
  }

  @Override 
  public void setDefault() {

  }

  @Override
  public ITransform createTransform(TransformMeta transformMeta, MQTTSubscriberData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
    return new MQTTSubscriber(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override 
  public MQTTSubscriberData getTransformData() {
    return new MQTTSubscriberData();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider )
      throws HopXmlException {
    broker = XmlHandler.getTagValue( transformNode, "BROKER" );
    String topics = XmlHandler.getTagValue( transformNode, "TOPICS" );
    this.topics = new ArrayList<>();
    if ( !Utils.isEmpty( topics ) ) {
      String[] parts = topics.split( "," );
      for ( String p : parts ) {
        this.topics.add( p.trim() );
      }
    }

    messageType = XmlHandler.getTagValue( transformNode, "MESSAGE_TYPE" );
    if ( Utils.isEmpty(messageType) ) {
      messageType = IValueMeta.getTypeDescription( IValueMeta.TYPE_STRING );
    }
    clientId = XmlHandler.getTagValue( transformNode, "CLIENT_ID" );
    timeout = XmlHandler.getTagValue( transformNode, "TIMEOUT" );
    keepAliveInterval = XmlHandler.getTagValue( transformNode, "KEEP_ALIVE" );
    executeForDuration = XmlHandler.getTagValue( transformNode, "EXECUTE_FOR_DURATION" );
    qos = XmlHandler.getTagValue( transformNode, "QOS" );
    requiresAuth = Boolean.parseBoolean( XmlHandler.getTagValue( transformNode, "REQUIRES_AUTH" ) );

    username = XmlHandler.getTagValue( transformNode, "USERNAME" );
    password = XmlHandler.getTagValue( transformNode, "PASSWORD" );
    if ( !Utils.isEmpty(password) ) {
      password = Encr.decryptPasswordOptionallyEncrypted(password);
    }

    String allowObjects = XmlHandler.getTagValue( transformNode, "READ_OBJECTS" );
    if ( !Utils.isEmpty( allowObjects ) ) {
      allowReadObjectMessageType = Boolean.parseBoolean( allowObjects );
    }

    Node sslNode = XmlHandler.getSubNode( transformNode, "SSL" );
    if ( sslNode != null ) {
      sslCaFile = XmlHandler.getTagValue( sslNode, "CA_FILE" );
      sslCertFile = XmlHandler.getTagValue( sslNode, "CERT_FILE" );
      sslKeyFile = XmlHandler.getTagValue( sslNode, "KEY_FILE" );
      sslKeyFilePass = XmlHandler.getTagValue( sslNode, "KEY_FILE_PASS" );
    }
  }

  @Override
  public String getXml() throws HopException {
    StringBuilder retval = new StringBuilder();
    if ( !Utils.isEmpty(broker) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "BROKER", broker) );
    }

    if ( !Utils.isEmpty(topics) ) {
      String topicString = "";
      for ( String t : topics) {
        topicString += "," + t;
      }
      retval.append( "    " ).append( XmlHandler.addTagValue( "TOPICS", topicString.substring( 1 ) ) );
    }

    if ( !Utils.isEmpty(messageType) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "MESSAGE_TYPE", messageType) );
    }

    if ( !Utils.isEmpty(clientId) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "CLIENT_ID", clientId) );
    }
    if ( !Utils.isEmpty(timeout) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "TIMEOUT", timeout) );
    }
    if ( !Utils.isEmpty(qos) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "QOS", qos) );
    }
    if ( !Utils.isEmpty(keepAliveInterval) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "KEEP_ALIVE", keepAliveInterval) );
    }
    if ( !Utils.isEmpty(executeForDuration) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "EXECUTE_FOR_DURATION", executeForDuration) );
    }
    retval.append( "    " ).append( XmlHandler.addTagValue( "REQUIRES_AUTH", Boolean.toString(requiresAuth) ) );
    if ( !Utils.isEmpty(username) ) {
      retval.append( "    " ).append( XmlHandler.addTagValue( "USERNAME", username) );
    }
    if ( !Utils.isEmpty(password) ) {
      retval.append( "    " )
          .append( XmlHandler.addTagValue( "PASSWORD", Encr.encryptPasswordIfNotUsingVariables(password) ) );
    }

    retval.append( "    " )
        .append( XmlHandler.addTagValue( "READ_OBJECTS", Boolean.toString(allowReadObjectMessageType) ) );

    if ( !Utils.isEmpty(sslCaFile) || !Utils.isEmpty(sslCertFile) || !Utils.isEmpty(sslKeyFile) || !Utils.isEmpty(sslKeyFilePass) ) {
      retval.append( "    " ).append( XmlHandler.openTag( "SSL" ) ).append( Const.CR );
      if ( !Utils.isEmpty(sslCaFile) ) {
        retval.append( "      " + XmlHandler.addTagValue( "CA_FILE", sslCaFile) );
      }
      if ( !Utils.isEmpty(sslCertFile) ) {
        retval.append( "      " + XmlHandler.addTagValue( "CERT_FILE", sslCertFile) );
      }
      if ( !Utils.isEmpty(sslKeyFile) ) {
        retval.append( "      " + XmlHandler.addTagValue( "KEY_FILE", sslKeyFile) );
      }
      if ( !Utils.isEmpty(sslKeyFilePass) ) {
        retval.append( "      " + XmlHandler.addTagValue( "KEY_FILE_PASS", sslKeyFilePass) );
      }
      retval.append( "    " ).append( XmlHandler.closeTag( "SSL" ) ).append( Const.CR );
    }

    return retval.toString();
  }

  /**
   * Gets the fields.
   *
   * @param inputRowMeta the input row meta that is modified in this method to reflect the output row metadata of the transform
   * @param name         Name of the transform to use as input for the origin field in the values
   * @param info         Fields used as extra lookup information
   * @param nextTransform     the next transform that is targeted
   * @param variables        the variables The variable variables to use to replace variables
   * @param metadataProvider    the MetaStore to use to load additional external data or metadata impacting the output fields
   * @throws HopTransformException the hop transform exception
   */
  public void getFields(IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                        IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    inputRowMeta.clear();
    try {
      inputRowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Topic", IValueMeta.TYPE_STRING ) );
      inputRowMeta.addValueMeta(
              ValueMetaFactory.createValueMeta( "Message", ValueMetaFactory.getIdForValueMeta( getMessageType() ) ) );
    } catch ( HopPluginException e ) {
      throw new HopTransformException( e );
    }
  }

}
