package org.apache.hop.beam.transforms.kafka;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.w3c.dom.Node;

@Transform(
        id = "BeamKafkaProduce",
        name = "Beam Kafka Produce",
        description = "Send messages to a Kafka Topic (Producer)",
        image = "beam-kafka-output.svg",
        categoryDescription = "Big Data",
        documentationUrl = "https://www.project-hop.org/manual/latest/plugins/transforms/beamproduce.html"
)
public class BeamProduceMeta extends BaseTransformMeta implements ITransformMeta<BeamProduce, DummyData> {

  public static final String BOOTSTRAP_SERVERS = "bootstrap_servers";
  public static final String TOPIC = "topic";
  public static final String KEY_FIELD = "key_field";
  public static final String MESSAGE_FIELD = "message_field";

  private String bootstrapServers;
  private String topic;
  private String keyField;
  private String messageField;

  public BeamProduceMeta() {
    super();
  }

  @Override public void setDefault() {
    bootstrapServers = "bootstrapServer1:9001,bootstrapServer2:9001";
    topic = "Topic1";
    keyField = "";
    messageField = "";
  }

  @Override public BeamProduce createTransform( TransformMeta transformMeta, DummyData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new BeamProduce( transformMeta, this, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public DummyData getTransformData() {
    return new DummyData();
  }

  @Override public String getDialogClassName() {
    return BeamProduceDialog.class.getName();
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextStep, IVariables variables, IHopMetadataProvider metadataProvider )
    throws HopTransformException {

    // No output
    //
    inputRowMeta.clear();
  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append( XmlHandler.addTagValue( BOOTSTRAP_SERVERS, bootstrapServers ) );
    xml.append( XmlHandler.addTagValue( TOPIC, topic ) );
    xml.append( XmlHandler.addTagValue( KEY_FIELD, keyField ) );
    xml.append( XmlHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    bootstrapServers = XmlHandler.getTagValue( transformNode, BOOTSTRAP_SERVERS );
    topic = XmlHandler.getTagValue( transformNode, TOPIC );
    keyField = XmlHandler.getTagValue( transformNode, KEY_FIELD );
    messageField = XmlHandler.getTagValue( transformNode, MESSAGE_FIELD );
  }


  /**
   * Gets topic
   *
   * @return value of topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic The topic to set
   */
  public void setTopic( String topic ) {
    this.topic = topic;
  }

  /**
   * Gets keyField
   *
   * @return value of keyField
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * @param keyField The keyField to set
   */
  public void setKeyField( String keyField ) {
    this.keyField = keyField;
  }

  /**
   * Gets messageField
   *
   * @return value of messageField
   */
  public String getMessageField() {
    return messageField;
  }

  /**
   * @param messageField The messageField to set
   */
  public void setMessageField( String messageField ) {
    this.messageField = messageField;
  }

  /**
   * Gets bootstrapServers
   *
   * @return value of bootstrapServers
   */
  public String getBootstrapServers() {
    return bootstrapServers;
  }

  /**
   * @param bootstrapServers The bootstrapServers to set
   */
  public void setBootstrapServers( String bootstrapServers ) {
    this.bootstrapServers = bootstrapServers;
  }
}
