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

package org.apache.hop.pipeline.transforms.mongodboutput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mongodb.MongoDbMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Class providing an output transform for writing data to a MongoDB collection. Supports insert,
 * truncate, upsert, multi-update (update all matching docs) and modifier update (update only
 * certain fields) operations. Can also create and drop indexes based on one or more fields.
 */
@Transform(
    id = "MongoDbOutput",
    image = "mongodb-output.svg",
    name = "i18n::MongoDbOutput.Name",
    description = "i18n::MongoDbOutput.Description",
    documentationUrl = "/pipeline/transforms/mongodboutput.html",
        keywords = "i18n::MongoDbOutputMeta.keyword",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output")
@InjectionSupported(
    localizationPrefix = "MongoDbOutput.Injection.",
    groups = {"FIELDS", "INDEXES"})
public class MongoDbOutputMeta extends MongoDbMeta<MongoDbOutput, MongoDbOutputData>
    implements ITransformMeta<MongoDbOutput, MongoDbOutputData> {

  private static final Class<?> PKG = MongoDbOutputMeta.class; // For Translator

  /** Class encapsulating paths to document fields */
  public static class MongoField {

    /** Incoming Hop field name */
    @Injection(name = "INCOMING_FIELD_NAME", group = "FIELDS")
    public String incomingFieldName = "";

    /** Contains the environment substituted field name updated once at init * */
    String environUpdatedFieldName = "";

    /** Dot separated path to the corresponding mongo field */
    @Injection(name = "MONGO_DOCUMENT_PATH", group = "FIELDS")
    public String mongoDocPath = "";

    /** Contains the environment substituted mongo doc path updated once at init * */
    String environUpdateMongoDocPath = "";

    protected List<String> pathList;
    protected List<String> tempPathList;

    /**
     * Whether to use the incoming field name as the mongo field key name. If false then the user
     * must supply the terminating field/key name.
     */
    @Injection(name = "INCOMING_AS_MONGO", group = "FIELDS")
    public boolean useIncomingFieldNameAsMongoFieldName;

    /** Whether this field is used in the query for an update operation */
    @Injection(name = "UPDATE_MATCH_FIELD", group = "FIELDS")
    public boolean updateMatchField;

    /**
     * Ignored if not doing a modifier update since all mongo paths are involved in a standard
     * upsert. If null/empty then this field is not being updated in the modifier update case.
     *
     * <p>$set $inc $push - append value to array (or set to [value] if field doesn't exist)
     *
     * <p>(support any others?)
     */
    @Injection(name = "MODIFIER_OPERATION", group = "FIELDS")
    public String modifierUpdateOperation = "N/A";

    /** Contains the environment substituted modifier operation updated once at init * */
    String environUpdateModifierOperation = "";

    /**
     * If a modifier opp, whether to apply on insert, update or both. Insert or update require
     * knowing whether matching record(s) exist, so require the overhead of a find().limit(1) query
     * for each row. The motivation for this is to allow a single document's structure to be created
     * and developed over multiple Hop rows. E.g. say a document is to contain an array of records
     * where each record in the array itself has a field with is an array. The $push operator
     * specifies the terminal array to add an element to via the dot notation. This terminal array
     * will be created if it does not already exist in the target document; however, it will not
     * create the intermediate array (i.e. the first array in the path). To do this requires a $set
     * operation that is executed only on insert (i.e. if the target document does not exist).
     * Whereas the $push operation should occur only on updates. A single operation can't combine
     * these two as it will result in a conflict (since they operate on the same array).
     */
    @Injection(name = "MODIFIER_POLICY", group = "FIELDS")
    public String modifierOperationApplyPolicy = "Insert&Update";

    /**
     * This flag determines the strategy of handling {@code null} values. By default, {@code null}s
     * are ignored. If this flag is set to {@code true}, the corresponding output value is sent as
     * {@code null}. <br>
     * Note: {@code null} and {@code undefined} are different values in Mongo!
     */
    @Injection(name = "INSERT_NULL", group = "FIELDS")
    public boolean insertNull = false;

    /**
     * If true, then the incoming Hop field value for this mongo field is expected to be of type
     * String and hold a Mongo doc fragment in JSON format. The doc fragment will be converted into
     * BSON and added into the overall document at the point specified by this MongoField's path
     */
    @Injection(name = "JSON", group = "FIELDS")
    public boolean inputJson = false;

    public MongoField copy() {
      MongoField mongoField = new MongoField();
      mongoField.incomingFieldName = incomingFieldName;
      mongoField.environUpdatedFieldName = environUpdatedFieldName;
      mongoField.mongoDocPath = mongoDocPath;
      mongoField.environUpdateMongoDocPath = environUpdateMongoDocPath;
      mongoField.useIncomingFieldNameAsMongoFieldName = useIncomingFieldNameAsMongoFieldName;
      mongoField.updateMatchField = updateMatchField;
      mongoField.modifierUpdateOperation = modifierUpdateOperation;
      mongoField.environUpdateModifierOperation = environUpdateModifierOperation;
      mongoField.modifierOperationApplyPolicy = modifierOperationApplyPolicy;
      mongoField.inputJson = inputJson;
      mongoField.insertNull = insertNull;

      return mongoField;
    }

    public void init(IVariables variables) {
      this.init(variables, true);
    }

    public void init(IVariables variables, boolean updateFromEnv) {
      if (updateFromEnv) {
        environUpdatedFieldName = variables.resolve(incomingFieldName);
        environUpdateMongoDocPath = variables.resolve(mongoDocPath);
        environUpdateModifierOperation = variables.resolve(modifierUpdateOperation);
      }
      pathList = new ArrayList<>();

      if (!StringUtils.isEmpty(environUpdateMongoDocPath)) {
        String[] parts = environUpdateMongoDocPath.split("\\.");
        for (String p : parts) {
          pathList.add(p);
        }
      }
      tempPathList = new ArrayList<>(pathList);
    }

    public void reset() {
      if (tempPathList != null && tempPathList.size() > 0) {
        tempPathList.clear();
      }
      if (tempPathList != null) {
        tempPathList.addAll(pathList);
      }
    }
  }

  /** Class encapsulating index definitions */
  public static class MongoIndex {

    /**
     * Dot notation for accessing a fields - e.g. person.address.street. Can also specify entire
     * embedded documents as an index (rather than a primitive key) - e.g. person.address.
     *
     * <p>Multiple fields are comma-separated followed by an optional "direction" indicator for the
     * index (1 or -1). If omitted, direction is assumed to be 1.
     */
    @Injection(name = "INDEX_FIELD", group = "INDEXES")
    public String pathToFields = "";

    /** whether to drop this index - default is create */
    @Injection(name = "DROP", group = "INDEXES")
    public boolean drop;

    // other options unique, sparse
    @Injection(name = "UNIQUE", group = "INDEXES")
    public boolean unique;

    @Injection(name = "SPARSE", group = "INDEXES")
    public boolean sparse;

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append(
          pathToFields
              + " (unique = "
              + Boolean.toString(unique)
              + " sparse = "
              + Boolean.toString(sparse)
              + ")");

      return buff.toString();
    }
  }

  /** Whether to truncate the collection */
  @Injection(name = "TRUNCATE")
  protected boolean truncate;

  /** True if updates (rather than inserts) are to be performed */
  @Injection(name = "UPDATE")
  protected boolean update;

  /** True if upserts are to be performed */
  @Injection(name = "UPSERT")
  protected boolean upsert;

  /** whether to update all records that match during an upsert or just the first */
  @Injection(name = "MULTI")
  protected boolean multi;

  /**
   * Modifier update involves updating only some fields and is efficient because of low network
   * overhead. Is also particularly efficient for $incr operations since the queried object does not
   * have to be returned in order to increment the field and then saved again.
   *
   * <p>If modifier update is false, then the standard update/insert operation is performed which
   * involves replacing the matched object with a new object involving all the user-defined mongo
   * paths
   */
  @Injection(name = "MODIFIER_UPDATE")
  protected boolean modifierUpdate;

  /** The batch size for inserts */
  @Injection(name = "BATCH_INSERT_SIZE")
  protected String batchInsertSize = "100";

  /** The list of paths to document fields for incoming Hop values */
  @InjectionDeep protected List<MongoField> mongoFields;

  /** The list of index definitions (if any) */
  @InjectionDeep protected List<MongoIndex> mongoIndexes;

  public static final int RETRIES = 5;
  public static final int RETRY_DELAY = 10; // seconds

  @Injection(name = "RETRY_NUMBER")
  private String writeRetries = "" + RETRIES;

  @Injection(name = "RETRY_DELAY")
  private String writeRetryDelay = "" + RETRY_DELAY; // seconds

  @Override
  public void setDefault() {
    setCollection("");
    upsert = false;
    modifierUpdate = true;
    truncate = false;
    batchInsertSize = "100";
  }

  /**
   * Set the list of document paths
   *
   * @param mongoFields the list of document paths
   */
  public void setMongoFields(List<MongoField> mongoFields) {
    this.mongoFields = mongoFields;
  }

  /**
   * Get the list of document paths
   *
   * @return the list of document paths
   */
  public List<MongoField> getMongoFields() {
    return mongoFields;
  }

  /**
   * Set the list of document indexes for creation/dropping
   *
   * @param mongoIndexes the list of indexes
   */
  public void setMongoIndexes(List<MongoIndex> mongoIndexes) {
    this.mongoIndexes = mongoIndexes;
  }

  /**
   * Get the list of document indexes for creation/dropping
   *
   * @return the list of indexes
   */
  public List<MongoIndex> getMongoIndexes() {
    return mongoIndexes;
  }

  /** @param r the number of retry attempts to make */
  public void setWriteRetries(String r) {
    writeRetries = r;
  }

  /**
   * Get the number of retry attempts to make if a particular write operation fails
   *
   * @return the number of retry attempts to make
   */
  public String getWriteRetries() {
    return writeRetries;
  }

  /**
   * Set the delay (in seconds) between write retry attempts
   *
   * @param d the delay in seconds between retry attempts
   */
  public void setWriteRetryDelay(String d) {
    writeRetryDelay = d;
  }

  /**
   * Get the delay (in seconds) between write retry attempts
   *
   * @return the delay in seconds between retry attempts
   */
  public String getWriteRetryDelay() {
    return writeRetryDelay;
  }

  /**
   * Set whether updates (rather than inserts) are to be performed
   *
   * @param update
   */
  public void setUpdate(boolean update) {
    this.update = update;
  }

  /**
   * Get whether updates (rather than inserts) are to be performed
   *
   * @return true if updates are to be performed
   */
  public boolean getUpdate() {
    return update;
  }

  /**
   * Set whether to upsert rather than update
   *
   * @param upsert true if we'll upsert rather than update
   */
  public void setUpsert(boolean upsert) {
    this.upsert = upsert;
  }

  /**
   * Get whether to upsert rather than update
   *
   * @return true if we'll upsert rather than update
   */
  public boolean getUpsert() {
    return upsert;
  }

  /**
   * Set whether the upsert should update all matching records rather than just the first.
   *
   * @param multi true if all matching records get updated when each row is upserted
   */
  public void setMulti(boolean multi) {
    this.multi = multi;
  }

  /**
   * Get whether the upsert should update all matching records rather than just the first.
   *
   * @return true if all matching records get updated when each row is upserted
   */
  public boolean getMulti() {
    return multi;
  }

  /**
   * Set whether the upsert operation is a modifier update - i.e where only specified fields in each
   * document get modified rather than a whole document replace.
   *
   * @param u true if the upsert operation is to be a modifier update
   */
  public void setModifierUpdate(boolean u) {
    modifierUpdate = u;
  }

  /**
   * Get whether the upsert operation is a modifier update - i.e where only specified fields in each
   * document get modified rather than a whole document replace.
   *
   * @return true if the upsert operation is to be a modifier update
   */
  public boolean getModifierUpdate() {
    return modifierUpdate;
  }

  /**
   * Set whether to truncate the collection before inserting
   *
   * @param truncate true if the all records in the collection are to be deleted
   */
  public void setTruncate(boolean truncate) {
    this.truncate = truncate;
  }

  /**
   * Get whether to truncate the collection before inserting
   *
   * @return true if the all records in the collection are to be deleted
   */
  public boolean getTruncate() {
    return truncate;
  }

  /**
   * Get the batch insert size
   *
   * @return the batch insert size
   */
  public String getBatchInsertSize() {
    return batchInsertSize;
  }

  /**
   * Set the batch insert size
   *
   * @param size the batch insert size
   */
  public void setBatchInsertSize(String size) {
    batchInsertSize = size;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    CheckResult cr;

    if ((prev == null) || (prev.size() == 0)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.Error.NotReceivingFieldsFromPreviousTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.ReceivingFields", prev.size()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.ReceivingInfo"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.Error.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public MongoDbOutput createTransform(
      TransformMeta transformMeta,
      MongoDbOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {

    return new MongoDbOutput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public MongoDbOutputData getTransformData() {
    return new MongoDbOutputData();
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append(XmlHandler.addTagValue("connection", connectionName));

    if (!StringUtils.isEmpty(getCollection())) {
      xml.append("\n    ").append(XmlHandler.addTagValue("mongo_collection", getCollection()));
    }
    if (!StringUtils.isEmpty(batchInsertSize)) {
      xml.append("\n    ").append(XmlHandler.addTagValue("batch_insert_size", batchInsertSize));
    }

    xml.append("\n    ").append(XmlHandler.addTagValue("truncate", truncate));
    xml.append("\n    ").append(XmlHandler.addTagValue("update", update));
    xml.append("\n    ").append(XmlHandler.addTagValue("upsert", upsert));
    xml.append("\n    ").append(XmlHandler.addTagValue("multi", multi));
    xml.append("\n    ").append(XmlHandler.addTagValue("modifier_update", modifierUpdate));

    xml.append("    ").append(XmlHandler.addTagValue("write_retries", writeRetries));
    xml.append("    ").append(XmlHandler.addTagValue("write_retry_delay", writeRetryDelay));

    if (mongoFields != null && mongoFields.size() > 0) {
      xml.append("\n    ").append(XmlHandler.openTag("mongo_fields"));

      for (MongoField field : mongoFields) {
        xml.append("\n      ").append(XmlHandler.openTag("mongo_field"));

        xml.append("\n         ")
            .append(XmlHandler.addTagValue("incoming_field_name", field.incomingFieldName));
        xml.append("\n         ")
            .append(XmlHandler.addTagValue("mongo_doc_path", field.mongoDocPath));
        xml.append("\n         ")
            .append(
                XmlHandler.addTagValue(
                    "use_incoming_field_name_as_mongo_field_name",
                    field.useIncomingFieldNameAsMongoFieldName));
        xml.append("\n         ")
            .append(XmlHandler.addTagValue("update_match_field", field.updateMatchField));
        xml.append("\n         ")
            .append(
                XmlHandler.addTagValue("modifier_update_operation", field.modifierUpdateOperation));
        xml.append("\n         ")
            .append(XmlHandler.addTagValue("modifier_policy", field.modifierOperationApplyPolicy));
        xml.append("\n         ").append(XmlHandler.addTagValue("json_field", field.inputJson));
        xml.append("\n         ").append(XmlHandler.addTagValue("allow_null", field.insertNull));

        xml.append("\n      ").append(XmlHandler.closeTag("mongo_field"));
      }

      xml.append("\n    ").append(XmlHandler.closeTag("mongo_fields"));
    }

    if (mongoIndexes != null && mongoIndexes.size() > 0) {
      xml.append("\n    ").append(XmlHandler.openTag("mongo_indexes"));

      for (MongoIndex index : mongoIndexes) {
        xml.append("\n      ").append(XmlHandler.openTag("mongo_index"));

        xml.append("\n         ")
            .append(XmlHandler.addTagValue("path_to_fields", index.pathToFields));
        xml.append("\n         ").append(XmlHandler.addTagValue("drop", index.drop));
        xml.append("\n         ").append(XmlHandler.addTagValue("unique", index.unique));
        xml.append("\n         ").append(XmlHandler.addTagValue("sparse", index.sparse));

        xml.append("\n      ").append(XmlHandler.closeTag("mongo_index"));
      }

      xml.append("\n    ").append(XmlHandler.closeTag("mongo_indexes"));
    }

    return xml.toString();
  }

  @Override
  public void loadXml(Node node, IHopMetadataProvider metaStore) throws HopXmlException {
    connectionName = XmlHandler.getTagValue(node, "connection");
    collection = XmlHandler.getTagValue(node, "mongo_collection");
    batchInsertSize = XmlHandler.getTagValue(node, "batch_insert_size");

    truncate = XmlHandler.getTagValue(node, "truncate").equalsIgnoreCase("Y");

    // for backwards compatibility with older metadata
    String update = XmlHandler.getTagValue(node, "update");
    if (!StringUtils.isEmpty(update)) {
      this.update = update.equalsIgnoreCase("Y");
    }

    upsert = XmlHandler.getTagValue(node, "upsert").equalsIgnoreCase("Y");
    multi = XmlHandler.getTagValue(node, "multi").equalsIgnoreCase("Y");
    modifierUpdate = XmlHandler.getTagValue(node, "modifier_update").equalsIgnoreCase("Y");

    // for backwards compatibility with older ktrs (to maintain correct
    // operation)
    if (upsert || multi) {
      this.update = true;
    }

    String writeRetries = XmlHandler.getTagValue(node, "write_retries");
    if (!StringUtils.isEmpty(writeRetries)) {
      this.writeRetries = writeRetries;
    }
    String writeRetryDelay = XmlHandler.getTagValue(node, "write_retry_delay");
    if (!StringUtils.isEmpty(writeRetryDelay)) {
      this.writeRetryDelay = writeRetryDelay;
    }

    Node fields = XmlHandler.getSubNode(node, "mongo_fields");
    if (fields != null && XmlHandler.countNodes(fields, "mongo_field") > 0) {
      int nrFields = XmlHandler.countNodes(fields, "mongo_field");
      mongoFields = new ArrayList<>();

      for (int i = 0; i < nrFields; i++) {
        Node fieldNode = XmlHandler.getSubNodeByNr(fields, "mongo_field", i);

        MongoField newField = new MongoField();
        newField.incomingFieldName = XmlHandler.getTagValue(fieldNode, "incoming_field_name");
        newField.mongoDocPath = XmlHandler.getTagValue(fieldNode, "mongo_doc_path");
        newField.useIncomingFieldNameAsMongoFieldName =
            XmlHandler.getTagValue(fieldNode, "use_incoming_field_name_as_mongo_field_name")
                .equalsIgnoreCase("Y");
        newField.updateMatchField =
            XmlHandler.getTagValue(fieldNode, "update_match_field").equalsIgnoreCase("Y");

        newField.modifierUpdateOperation =
            XmlHandler.getTagValue(fieldNode, "modifier_update_operation");
        String policy = XmlHandler.getTagValue(fieldNode, "modifier_policy");
        if (!StringUtils.isEmpty(policy)) {
          newField.modifierOperationApplyPolicy = policy;
        }
        String jsonField = XmlHandler.getTagValue(fieldNode, "json_field");
        if (!StringUtils.isEmpty(jsonField)) {
          newField.inputJson = jsonField.equalsIgnoreCase("Y");
        }
        String allowNull = XmlHandler.getTagValue(fieldNode, "allow_null");
        newField.insertNull = "Y".equalsIgnoreCase(allowNull);

        mongoFields.add(newField);
      }
    }

    fields = XmlHandler.getSubNode(node, "mongo_indexes");
    if (fields != null && XmlHandler.countNodes(fields, "mongo_index") > 0) {
      int nrFields = XmlHandler.countNodes(fields, "mongo_index");

      mongoIndexes = new ArrayList<>();

      for (int i = 0; i < nrFields; i++) {
        Node fieldNode = XmlHandler.getSubNodeByNr(fields, "mongo_index", i);

        MongoIndex newIndex = new MongoIndex();

        newIndex.pathToFields = XmlHandler.getTagValue(fieldNode, "path_to_fields");
        newIndex.drop = XmlHandler.getTagValue(fieldNode, "drop").equalsIgnoreCase("Y");
        newIndex.unique = XmlHandler.getTagValue(fieldNode, "unique").equalsIgnoreCase("Y");
        newIndex.sparse = XmlHandler.getTagValue(fieldNode, "sparse").equalsIgnoreCase("Y");

        mongoIndexes.add(newIndex);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.BaseTransformMeta#getDialogClassName()
   */
  @Override
  public String getDialogClassName() {
    return MongoDbOutputDialog.class.getCanonicalName();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.BaseTransformMeta#supportsErrorHandling()
   */
  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
