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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mongodb.MongoDbMeta;
import org.w3c.dom.Node;

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
@Getter
@Setter
public class MongoDbOutputMeta extends MongoDbMeta<MongoDbOutput, MongoDbOutputData> {

  private static final Class<?> PKG = MongoDbOutputMeta.class;

  /** Class encapsulating paths to document fields */
  @Getter
  @Setter
  public static class MongoField {

    /** Incoming Hop field name */
    @HopMetadataProperty(key = "incoming_field_name", injectionKey = "INCOMING_FIELD_NAME")
    public String incomingFieldName = "";

    /** Contains the environment substituted field name updated once at init * */
    String environUpdatedFieldName = "";

    /** Dot separated path to the corresponding mongo field */
    @HopMetadataProperty(key = "mongo_doc_path", injectionKey = "MONGO_DOCUMENT_PATH")
    public String mongoDocPath = "";

    /** Contains the environment substituted mongo doc path updated once at init * */
    String environUpdateMongoDocPath = "";

    protected List<String> pathList;
    protected List<String> tempPathList;

    /**
     * Whether to use the incoming field name as the mongo field key name. If false then the user
     * must supply the terminating field/key name.
     */
    @HopMetadataProperty(
        key = "use_incoming_field_name_as_mongo_field_name",
        injectionKey = "INCOMING_AS_MONGO")
    public boolean useIncomingFieldNameAsMongoFieldName;

    /** Whether this field is used in the query for an update operation */
    @HopMetadataProperty(key = "update_match_field", injectionKey = "UPDATE_MATCH_FIELD")
    public boolean updateMatchField;

    /**
     * Ignored if not doing a modifier update since all mongo paths are involved in a standard
     * upsert. If null/empty then this field is not being updated in the modifier update case.
     *
     * <p>$set $inc $push - append value to array (or set to [value] if field doesn't exist)
     *
     * <p>(support any others?)
     */
    @HopMetadataProperty(key = "modifier_update_operation", injectionKey = "MODIFIER_OPERATION")
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
    @HopMetadataProperty(key = "modifier_policy", injectionKey = "MODIFIER_POLICY")
    public String modifierOperationApplyPolicy = "Insert&Update";

    /**
     * This flag determines the strategy of handling {@code null} values. By default, {@code null}s
     * are ignored. If this flag is set to {@code true}, the corresponding output value is sent as
     * {@code null}. <br>
     * Note: {@code null} and {@code undefined} are different values in Mongo!
     */
    @HopMetadataProperty(key = "allow_null", injectionKey = "INSERT_NULL")
    public boolean insertNull = false;

    /**
     * If true, then the incoming Hop field value for this mongo field is expected to be of type
     * String and hold a Mongo doc fragment in JSON format. The doc fragment will be converted into
     * BSON and added into the overall document at the point specified by this MongoField's path
     */
    @HopMetadataProperty(key = "json_field", injectionKey = "JSON")
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
        Collections.addAll(pathList, parts);
      }
      tempPathList = new ArrayList<>(pathList);
    }

    public void reset() {
      if (!Utils.isEmpty(tempPathList)) {
        tempPathList.clear();
      }
      if (tempPathList != null) {
        tempPathList.addAll(pathList);
      }
    }
  }

  /** Class encapsulating index definitions */
  @Getter
  @Setter
  public static class MongoIndex {

    /**
     * Dot notation for accessing a fields - e.g. person.address.street. Can also specify entire
     * embedded documents as an index (rather than a primitive key) - e.g. person.address.
     *
     * <p>Multiple fields are comma-separated followed by an optional "direction" indicator for the
     * index (1 or -1). If omitted, direction is assumed to be 1.
     */
    @HopMetadataProperty(key = "path_to_fields", injectionKey = "INDEX_FIELD")
    public String pathToFields = "";

    /** whether to drop this index - default is create */
    @HopMetadataProperty(key = "drop", injectionKey = "DROP")
    public boolean drop;

    // other options unique, sparse
    @HopMetadataProperty(key = "unique", injectionKey = "UNIQUE")
    public boolean unique;

    @HopMetadataProperty(key = "sparse", injectionKey = "SPARSE")
    public boolean sparse;

    @Override
    public String toString() {
      return pathToFields + " (unique = " + unique + " sparse = " + sparse + ")";
    }
  }

  /** Whether to truncate the collection */
  @HopMetadataProperty(key = "truncate", injectionKey = "TRUNCATE")
  protected boolean truncate;

  /** True if updates (rather than inserts) are to be performed */
  @HopMetadataProperty(key = "update", injectionKey = "UPDATE")
  protected boolean update;

  /** True if upserts are to be performed */
  @HopMetadataProperty(key = "upsert", injectionKey = "UPSERT")
  protected boolean upsert;

  /** whether to update all records that match during an upsert or just the first */
  @HopMetadataProperty(key = "multi", injectionKey = "MULTI")
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
  @HopMetadataProperty(key = "modifier_update", injectionKey = "MODIFIER_UPDATE")
  protected boolean modifierUpdate;

  /** The batch size for inserts */
  @HopMetadataProperty(key = "batch_insert_size", injectionKey = "BATCH_INSERT_SIZE")
  protected String batchInsertSize = "100";

  /** The list of paths to document fields for incoming Hop values */
  @HopMetadataProperty(
      groupKey = "mongo_fields",
      key = "mongo_field",
      injectionKey = "FIELDS",
      injectionGroupKey = "FIELD")
  protected List<MongoField> mongoFields;

  /** The list of index definitions (if any) */
  @HopMetadataProperty(
      groupKey = "mongo_indexes",
      key = "mongo_index",
      injectionKey = "INDEXES",
      injectionGroupKey = "INDEX")
  protected List<MongoIndex> mongoIndexes;

  public static final int RETRIES = 5;
  public static final int RETRY_DELAY = 10; // seconds

  @HopMetadataProperty(key = "write_retries", injectionKey = "RETRY_NUMBER")
  private String writeRetries = "" + RETRIES;

  @HopMetadataProperty(key = "write_retry_delay", injectionKey = "RETRY_DELAY")
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
   * Added for backwards compatibility
   *
   * @param transformNode XML Node of the transform
   * @param metadataProvider metadata provider
   * @throws HopXmlException when unable to load from XML
   */
  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);
    String tempCollection = XmlHandler.getTagValue(transformNode, "mongo_collection");
    if (!Utils.isEmpty(tempCollection)) {
      setCollection(tempCollection);
    }
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

    if ((prev == null) || (prev.isEmpty())) {
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
