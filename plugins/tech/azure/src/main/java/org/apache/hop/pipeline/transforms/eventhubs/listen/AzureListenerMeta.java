package org.apache.hop.pipeline.transforms.eventhubs.listen;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
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

@Transform(
    id = "AzureListener",
    name = "Azure Event Hubs Listener",
    description = "Listen to a Microsoft Azure Event Hub and read from it",
    image = "event-hubs-listener.svg",
    categoryDescription = "Streaming",
    documentationUrl =
        "https://github.com/mattcasters/kettle-azure-event-hubs/wiki/Microsoft-Azure-Event-Hubs-Listener")
public class AzureListenerMeta extends BaseTransformMeta
    implements ITransformMeta<AzureListener, AzureListenerData> {

  public static final String NAMESPACE = "namespace";
  public static final String EVENT_HUB_NAME = "event_hub_name";
  public static final String SAS_KEY_NAME = "sas_key_name";
  public static final String SAS_KEY = "sas_key";
  public static final String BATCH_SIZE = "batch_size";
  public static final String PREFETCH_SIZE = "prefetch_size";
  public static final String OUTPUT_FIELD = "output_field";
  public static final String PARTITION_ID_FIELD = "partition_id_field";
  public static final String OFFSET_FIELD = "offset_field";
  public static final String SEQUENCE_NUMBER_FIELD = "sequence_number_field";
  public static final String HOST_FIELD = "host_field";
  public static final String ENQUEUED_TIME_FIELD = "enqueued_time_field";
  public static final String BATCH_TRANSFORMATION = "batch_transformation";
  public static final String BATCH_INPUT_Transform = "batch_input_Transform";
  public static final String BATCH_OUTPUT_Transform = "batch_output_Transform";
  public static final String BATCH_MAX_WAIT_TIME = "batch_max_wait_time";

  public static final String CONSUMER_GROUP_NAME = "consumer_group_name";
  public static final String EVENT_HUB_CONNECTION_STRING = "event_hub_connection_string";
  public static final String STORAGE_CONNECTION_STRING = "storage_connection_string";
  public static final String STORAGE_CONTAINER_NAME = "storage_container_name";

  private String namespace;
  private String eventHubName;
  private String sasKeyName;
  private String sasKey;
  private String consumerGroupName;
  private String storageConnectionString;
  private String storageContainerName;

  private String prefetchSize;
  private String batchSize;

  private String outputField;
  private String partitionIdField;
  private String offsetField;
  private String sequenceNumberField;
  private String hostField;
  private String enqueuedTimeField;

  private String batchPipeline;
  private String batchInputTransform;
  private String batchOutputTransform;
  private String batchMaxWaitTime;

  public AzureListenerMeta() {
    super();
  }

  @Override
  public void setDefault() {
    consumerGroupName = "$Default";
    outputField = "message";
    partitionIdField = "partitionId";
    offsetField = "offset";
    sequenceNumberField = "sequenceNumber";
    hostField = "host";
    enqueuedTimeField = "enqueuedTime";
  }

  @Override public ITransform createTransform( TransformMeta transformMeta, AzureListenerData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new AzureListener(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public AzureListenerData getTransformData() {
    return new AzureListenerData();
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (StringUtils.isNotEmpty( batchPipeline )
        && StringUtils.isNotEmpty(batchInputTransform)) {
      // Load the transformation, get the Transform output fields...
      //
      try {
        PipelineMeta batchTransMeta = loadBatchPipelineMeta(this, metadataProvider, variables);
        IRowMeta TransformFields =
            batchTransMeta.getTransformFields(variables, variables.resolve(batchOutputTransform));
        rowMeta.clear();
        rowMeta.addRowMeta(TransformFields);
        return;
      } catch (Exception e) {
        throw new HopTransformException(
            "Unable to get fields from batch pipeline Transform " + batchOutputTransform, e);
      }
    }

    getRegularRowMeta(rowMeta, variables);
  }

  public void getRegularRowMeta(IRowMeta rowMeta, IVariables variables) {
    // Output message field name
    //
    String outputFieldName = variables.resolve(outputField);
    if (StringUtils.isNotEmpty(outputFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaString(outputFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The partition ID field name
    //
    String partitionIdFieldName = variables.resolve(partitionIdField);
    if (StringUtils.isNotEmpty(partitionIdFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaString(partitionIdFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The offset field name
    //
    String offsetFieldName = variables.resolve(offsetField);
    if (StringUtils.isNotEmpty(offsetFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaString(offsetFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The sequence number field name
    //
    String sequenceNumberFieldName = variables.resolve(sequenceNumberField);
    if (StringUtils.isNotEmpty(sequenceNumberFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaInteger(sequenceNumberFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The host field name
    //
    String hostFieldName = variables.resolve(hostField);
    if (StringUtils.isNotEmpty(hostFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaString(hostFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The enqueued time field name
    //
    String enqueuedTimeFieldName = variables.resolve( enqueuedTimeField );
    if (StringUtils.isNotEmpty(enqueuedTimeFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaTimestamp(enqueuedTimeFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();
    xml.append(XmlHandler.addTagValue(NAMESPACE, namespace));
    xml.append(XmlHandler.addTagValue(EVENT_HUB_NAME, eventHubName));
    xml.append(XmlHandler.addTagValue(SAS_KEY_NAME, sasKeyName));
    xml.append(XmlHandler.addTagValue(SAS_KEY, Encr.encryptPasswordIfNotUsingVariables(sasKey)));
    xml.append(XmlHandler.addTagValue(BATCH_SIZE, batchSize));
    xml.append(XmlHandler.addTagValue(PREFETCH_SIZE, prefetchSize));
    xml.append(XmlHandler.addTagValue(OUTPUT_FIELD, outputField));
    xml.append(XmlHandler.addTagValue(PARTITION_ID_FIELD, partitionIdField));
    xml.append(XmlHandler.addTagValue(OFFSET_FIELD, offsetField));
    xml.append(XmlHandler.addTagValue(SEQUENCE_NUMBER_FIELD, sequenceNumberField));
    xml.append(XmlHandler.addTagValue(HOST_FIELD, hostField));
    xml.append(XmlHandler.addTagValue( ENQUEUED_TIME_FIELD, enqueuedTimeField ));
    xml.append(XmlHandler.addTagValue(CONSUMER_GROUP_NAME, consumerGroupName));
    xml.append(XmlHandler.addTagValue(STORAGE_CONNECTION_STRING, storageConnectionString));
    xml.append(XmlHandler.addTagValue(STORAGE_CONTAINER_NAME, storageContainerName));
    xml.append(XmlHandler.addTagValue(BATCH_TRANSFORMATION, batchPipeline ));
    xml.append(XmlHandler.addTagValue(BATCH_INPUT_Transform, batchInputTransform));
    xml.append(XmlHandler.addTagValue(BATCH_OUTPUT_Transform, batchOutputTransform));
    xml.append(XmlHandler.addTagValue(BATCH_MAX_WAIT_TIME, batchMaxWaitTime));

    return xml.toString();
  }

  @Override
  public void loadXml(Node Transformnode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    namespace = XmlHandler.getTagValue(Transformnode, NAMESPACE);
    eventHubName = XmlHandler.getTagValue(Transformnode, EVENT_HUB_NAME);
    sasKeyName = XmlHandler.getTagValue(Transformnode, SAS_KEY_NAME);
    sasKey =
        Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(Transformnode, SAS_KEY));
    batchSize = XmlHandler.getTagValue(Transformnode, BATCH_SIZE);
    prefetchSize = XmlHandler.getTagValue(Transformnode, PREFETCH_SIZE);
    outputField = XmlHandler.getTagValue(Transformnode, OUTPUT_FIELD);
    partitionIdField = XmlHandler.getTagValue(Transformnode, PARTITION_ID_FIELD);
    offsetField = XmlHandler.getTagValue(Transformnode, OFFSET_FIELD);
    sequenceNumberField = XmlHandler.getTagValue(Transformnode, SEQUENCE_NUMBER_FIELD);
    hostField = XmlHandler.getTagValue(Transformnode, HOST_FIELD);
    enqueuedTimeField = XmlHandler.getTagValue(Transformnode, ENQUEUED_TIME_FIELD );
    consumerGroupName = XmlHandler.getTagValue(Transformnode, CONSUMER_GROUP_NAME);
    storageConnectionString = XmlHandler.getTagValue(Transformnode, STORAGE_CONNECTION_STRING);
    storageContainerName = XmlHandler.getTagValue(Transformnode, STORAGE_CONTAINER_NAME);
    batchPipeline = XmlHandler.getTagValue(Transformnode, BATCH_TRANSFORMATION);
    batchInputTransform = XmlHandler.getTagValue(Transformnode, BATCH_INPUT_Transform);
    batchOutputTransform = XmlHandler.getTagValue(Transformnode, BATCH_OUTPUT_Transform);
    batchMaxWaitTime = XmlHandler.getTagValue(Transformnode, BATCH_MAX_WAIT_TIME);
    super.loadXml(Transformnode, metadataProvider);
  }

  public static final synchronized PipelineMeta loadBatchPipelineMeta(
      AzureListenerMeta azureListenerMeta,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    PipelineMeta batchPipelineMeta;

    String realFilename = variables.resolve(azureListenerMeta.getBatchPipeline());
    try {
      // OK, load the meta-data from file...
      //
      // Don't set internal variables: they belong to the parent thread!
      //
      batchPipelineMeta = new PipelineMeta(realFilename, metadataProvider, false, variables);
      batchPipelineMeta
          .getLogChannel()
          .logDetailed("Batch pipeline was loaded from XML '" + realFilename + "'");
    } catch (Exception e) {
      throw new HopException("Unable to load batch pipeline", e);
    }

    return batchPipelineMeta;
  }

  /**
   * @return The objects referenced in the Transform, like a mapping, a transformation, a job, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      "Batch pipeline",
    };
  }

  private boolean isPipelineDefined() {
    return StringUtils.isNotEmpty( batchPipeline );
  }

  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {
      isPipelineDefined(),
    };
  }

  /**
   * Load the referenced object
   *
   * @param index the object index to load
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public IHasFilename loadReferencedObject( int index, IHopMetadataProvider metadataProvider, IVariables variables )
    throws HopException {
    return loadBatchPipelineMeta( this, metadataProvider, variables );
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getEventHubName() {
    return eventHubName;
  }

  public void setEventHubName(String eventHubName) {
    this.eventHubName = eventHubName;
  }

  public String getSasKeyName() {
    return sasKeyName;
  }

  public void setSasKeyName(String sasKeyName) {
    this.sasKeyName = sasKeyName;
  }

  public String getSasKey() {
    return sasKey;
  }

  public void setSasKey(String sasKey) {
    this.sasKey = sasKey;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(String outputField) {
    this.outputField = outputField;
  }

  public String getConsumerGroupName() {
    return consumerGroupName;
  }

  public void setConsumerGroupName(String consumerGroupName) {
    this.consumerGroupName = consumerGroupName;
  }

  public String getStorageConnectionString() {
    return storageConnectionString;
  }

  public void setStorageConnectionString(String storageConnectionString) {
    this.storageConnectionString = storageConnectionString;
  }

  public String getStorageContainerName() {
    return storageContainerName;
  }

  public void setStorageContainerName(String storageContainerName) {
    this.storageContainerName = storageContainerName;
  }

  public String getPrefetchSize() {
    return prefetchSize;
  }

  public void setPrefetchSize(String prefetchSize) {
    this.prefetchSize = prefetchSize;
  }

  public String getPartitionIdField() {
    return partitionIdField;
  }

  public void setPartitionIdField(String partitionIdField) {
    this.partitionIdField = partitionIdField;
  }

  public String getOffsetField() {
    return offsetField;
  }

  public void setOffsetField(String offsetField) {
    this.offsetField = offsetField;
  }

  public String getSequenceNumberField() {
    return sequenceNumberField;
  }

  public void setSequenceNumberField(String sequenceNumberField) {
    this.sequenceNumberField = sequenceNumberField;
  }

  public String getHostField() {
    return hostField;
  }

  public void setHostField(String hostField) {
    this.hostField = hostField;
  }

  public String getEnqueuedTimeField() {
    return enqueuedTimeField;
  }

  public void setEnqueuedTimeField( String enqueuedTimeField ) {
    this.enqueuedTimeField = enqueuedTimeField;
  }

  public String getBatchPipeline() {
    return batchPipeline;
  }

  public void setBatchPipeline( String batchPipeline ) {
    this.batchPipeline = batchPipeline;
  }

  public String getBatchInputTransform() {
    return batchInputTransform;
  }

  public void setBatchInputTransform(String batchInputTransform) {
    this.batchInputTransform = batchInputTransform;
  }

  public String getBatchOutputTransform() {
    return batchOutputTransform;
  }

  public void setBatchOutputTransform(String batchOutputTransform) {
    this.batchOutputTransform = batchOutputTransform;
  }

  public String getBatchMaxWaitTime() {
    return batchMaxWaitTime;
  }

  public void setBatchMaxWaitTime(String batchMaxWaitTime) {
    this.batchMaxWaitTime = batchMaxWaitTime;
  }
}
