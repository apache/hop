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
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
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
    name = "MongoDB output",
    description = "Writes to a Mongo DB collection",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/mongodboutput.html",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output")
@InjectionSupported(
    localizationPrefix = "MongoDbOutput.Injection.",
    groups = {"FIELDS", "INDEXES"})
public class MongoDbOutputMeta extends MongoDbMeta<MongoDbOutput, MongoDbOutputData>
    implements ITransformMeta<MongoDbOutput, MongoDbOutputData> {

  private static Class<?> PKG = MongoDbOutputMeta.class; // For Translator

  /** Class encapsulating paths to document fields */
  public static class MongoField {

    /** Incoming Hop field name */
    @Injection(name = "INCOMING_FIELD_NAME", group = "FIELDS")
    public String m_incomingFieldName = "";

    /** Contains the environment substituted field name updated once at init * */
    String environUpdatedFieldName = "";

    /** Dot separated path to the corresponding mongo field */
    @Injection(name = "MONGO_DOCUMENT_PATH", group = "FIELDS")
    public String m_mongoDocPath = "";

    /** Contains the environment substituted mongo doc path updated once at init * */
    String environUpdateMongoDocPath = "";

    protected List<String> m_pathList;
    protected List<String> m_tempPathList;

    /**
     * Whether to use the incoming field name as the mongo field key name. If false then the user
     * must supply the terminating field/key name.
     */
    @Injection(name = "INCOMING_AS_MONGO", group = "FIELDS")
    public boolean m_useIncomingFieldNameAsMongoFieldName;

    /** Whether this field is used in the query for an update operation */
    @Injection(name = "UPDATE_MATCH_FIELD", group = "FIELDS")
    public boolean m_updateMatchField;

    /**
     * Ignored if not doing a modifier update since all mongo paths are involved in a standard
     * upsert. If null/empty then this field is not being updated in the modifier update case.
     *
     * <p>$set $inc $push - append value to array (or set to [value] if field doesn't exist)
     *
     * <p>(support any others?)
     */
    @Injection(name = "MODIFIER_OPERATION", group = "FIELDS")
    public String m_modifierUpdateOperation = "N/A";

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
    public String m_modifierOperationApplyPolicy = "Insert&Update";

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
    public boolean m_JSON = false;

    public MongoField copy() {
      MongoField newF = new MongoField();
      newF.m_incomingFieldName = m_incomingFieldName;
      newF.environUpdatedFieldName = environUpdatedFieldName;
      newF.m_mongoDocPath = m_mongoDocPath;
      newF.environUpdateMongoDocPath = environUpdateMongoDocPath;
      newF.m_useIncomingFieldNameAsMongoFieldName = m_useIncomingFieldNameAsMongoFieldName;
      newF.m_updateMatchField = m_updateMatchField;
      newF.m_modifierUpdateOperation = m_modifierUpdateOperation;
      newF.environUpdateModifierOperation = environUpdateModifierOperation;
      newF.m_modifierOperationApplyPolicy = m_modifierOperationApplyPolicy;
      newF.m_JSON = m_JSON;
      newF.insertNull = insertNull;

      return newF;
    }

    public void init(IVariables vars) {
      this.init(vars, true);
    }

    public void init(IVariables vars, boolean updateFromEnv) {
      if (updateFromEnv) {
        environUpdatedFieldName = vars.resolve(m_incomingFieldName);
        environUpdateMongoDocPath = vars.resolve(m_mongoDocPath);
        environUpdateModifierOperation = vars.resolve(m_modifierUpdateOperation);
      }
      m_pathList = new ArrayList<>();

      if (!StringUtils.isEmpty(environUpdateMongoDocPath)) {
        String[] parts = environUpdateMongoDocPath.split("\\.");
        for (String p : parts) {
          m_pathList.add(p);
        }
      }
      m_tempPathList = new ArrayList<>( m_pathList );
    }

    public void reset() {
      if (m_tempPathList != null && m_tempPathList.size() > 0) {
        m_tempPathList.clear();
      }
      if (m_tempPathList != null) {
        m_tempPathList.addAll(m_pathList);
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
    public String m_pathToFields = "";

    /** whether to drop this index - default is create */
    @Injection(name = "DROP", group = "INDEXES")
    public boolean m_drop;

    // other options unique, sparse
    @Injection(name = "UNIQUE", group = "INDEXES")
    public boolean m_unique;

    @Injection(name = "SPARSE", group = "INDEXES")
    public boolean m_sparse;

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append(
          m_pathToFields
              + " (unique = "
              + new Boolean(m_unique).toString()
              + " sparse = "
              + new Boolean(m_sparse).toString()
              + ")");

      return buff.toString();
    }
  }

  /** Whether to truncate the collection */
  @Injection(name = "TRUNCATE")
  protected boolean m_truncate;

  /** True if updates (rather than inserts) are to be performed */
  @Injection(name = "UPDATE")
  protected boolean m_update;

  /** True if upserts are to be performed */
  @Injection(name = "UPSERT")
  protected boolean m_upsert;

  /** whether to update all records that match during an upsert or just the first */
  @Injection(name = "MULTI")
  protected boolean m_multi;

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
  protected boolean m_modifierUpdate;

  /** The batch size for inserts */
  @Injection(name = "BATCH_INSERT_SIZE")
  protected String m_batchInsertSize = "100";

  /** The list of paths to document fields for incoming Hop values */
  @InjectionDeep protected List<MongoField> m_mongoFields;

  /** The list of index definitions (if any) */
  @InjectionDeep protected List<MongoIndex> m_mongoIndexes;

  public static final int RETRIES = 5;
  public static final int RETRY_DELAY = 10; // seconds

  @Injection(name = "RETRY_NUMBER")
  private String m_writeRetries = "" + RETRIES;

  @Injection(name = "RETRY_DELAY")
  private String m_writeRetryDelay = "" + RETRY_DELAY; // seconds

  @Override
  public void setDefault() {
    setHostnames("localhost");
    setPort("27017");
    setCollection("");
    setDbName("");
    setAuthenticationMechanism("");
    m_upsert = false;
    m_modifierUpdate = false;
    m_truncate = false;
    m_batchInsertSize = "100";
    setWriteConcern("");
    setWTimeout("");
    setJournal(false);
  }

  /**
   * Set the list of document paths
   *
   * @param mongoFields the list of document paths
   */
  public void setMongoFields(List<MongoField> mongoFields) {
    m_mongoFields = mongoFields;
  }

  /**
   * Get the list of document paths
   *
   * @return the list of document paths
   */
  public List<MongoField> getMongoFields() {
    return m_mongoFields;
  }

  /**
   * Set the list of document indexes for creation/dropping
   *
   * @param mongoIndexes the list of indexes
   */
  public void setMongoIndexes(List<MongoIndex> mongoIndexes) {
    m_mongoIndexes = mongoIndexes;
  }

  /**
   * Get the list of document indexes for creation/dropping
   *
   * @return the list of indexes
   */
  public List<MongoIndex> getMongoIndexes() {
    return m_mongoIndexes;
  }

  /** @param r the number of retry attempts to make */
  public void setWriteRetries(String r) {
    m_writeRetries = r;
  }

  /**
   * Get the number of retry attempts to make if a particular write operation fails
   *
   * @return the number of retry attempts to make
   */
  public String getWriteRetries() {
    return m_writeRetries;
  }

  /**
   * Set the delay (in seconds) between write retry attempts
   *
   * @param d the delay in seconds between retry attempts
   */
  public void setWriteRetryDelay(String d) {
    m_writeRetryDelay = d;
  }

  /**
   * Get the delay (in seconds) between write retry attempts
   *
   * @return the delay in seconds between retry attempts
   */
  public String getWriteRetryDelay() {
    return m_writeRetryDelay;
  }

  /**
   * Set whether updates (rather than inserts) are to be performed
   *
   * @param update
   */
  public void setUpdate(boolean update) {
    m_update = update;
  }

  /**
   * Get whether updates (rather than inserts) are to be performed
   *
   * @return true if updates are to be performed
   */
  public boolean getUpdate() {
    return m_update;
  }

  /**
   * Set whether to upsert rather than update
   *
   * @param upsert true if we'll upsert rather than update
   */
  public void setUpsert(boolean upsert) {
    m_upsert = upsert;
  }

  /**
   * Get whether to upsert rather than update
   *
   * @return true if we'll upsert rather than update
   */
  public boolean getUpsert() {
    return m_upsert;
  }

  /**
   * Set whether the upsert should update all matching records rather than just the first.
   *
   * @param multi true if all matching records get updated when each row is upserted
   */
  public void setMulti(boolean multi) {
    m_multi = multi;
  }

  /**
   * Get whether the upsert should update all matching records rather than just the first.
   *
   * @return true if all matching records get updated when each row is upserted
   */
  public boolean getMulti() {
    return m_multi;
  }

  /**
   * Set whether the upsert operation is a modifier update - i.e where only specified fields in each
   * document get modified rather than a whole document replace.
   *
   * @param u true if the upsert operation is to be a modifier update
   */
  public void setModifierUpdate(boolean u) {
    m_modifierUpdate = u;
  }

  /**
   * Get whether the upsert operation is a modifier update - i.e where only specified fields in each
   * document get modified rather than a whole document replace.
   *
   * @return true if the upsert operation is to be a modifier update
   */
  public boolean getModifierUpdate() {
    return m_modifierUpdate;
  }

  /**
   * Set whether to truncate the collection before inserting
   *
   * @param truncate true if the all records in the collection are to be deleted
   */
  public void setTruncate(boolean truncate) {
    m_truncate = truncate;
  }

  /**
   * Get whether to truncate the collection before inserting
   *
   * @return true if the all records in the collection are to be deleted
   */
  public boolean getTruncate() {
    return m_truncate;
  }

  /**
   * Get the batch insert size
   *
   * @return the batch insert size
   */
  public String getBatchInsertSize() {
    return m_batchInsertSize;
  }

  /**
   * Set the batch insert size
   *
   * @param size the batch insert size
   */
  public void setBatchInsertSize(String size) {
    m_batchInsertSize = size;
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
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.Error.NotReceivingFieldsFromPreviousTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.ReceivingFields", prev.size()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.ReceivingInfo"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
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

    if (!StringUtils.isEmpty(getHostnames())) {
      xml.append("\n    ").append(XmlHandler.addTagValue("mongo_host", getHostnames()));
    }
    if (!StringUtils.isEmpty(getPort())) {
      xml.append("\n    ").append(XmlHandler.addTagValue("mongo_port", getPort()));
    }

    xml.append("    ")
        .append(XmlHandler.addTagValue("use_all_replica_members", getUseAllReplicaSetMembers()));

    if (!StringUtils.isEmpty(getAuthenticationDatabaseName())) {
      xml.append("\n    ")
          .append(XmlHandler.addTagValue("mongo_auth_database", getAuthenticationDatabaseName()));
    }
    if (!StringUtils.isEmpty(getAuthenticationUser())) {
      xml.append("\n    ").append(XmlHandler.addTagValue("mongo_user", getAuthenticationUser()));
    }
    if (!StringUtils.isEmpty(getAuthenticationPassword())) {
      xml.append("\n    ")
          .append(
              XmlHandler.addTagValue(
                  "mongo_password",
                  Encr.encryptPasswordIfNotUsingVariables(getAuthenticationPassword())));
    }
    xml.append("    ").append(XmlHandler.addTagValue("auth_mech", getAuthenticationMechanism()));

    xml.append("    ")
        .append(XmlHandler.addTagValue("auth_kerberos", getUseKerberosAuthentication()));

    if (!StringUtils.isEmpty(getDbName())) {
      xml.append("\n    ").append(XmlHandler.addTagValue("mongo_db", getDbName()));
    }
    if (!StringUtils.isEmpty(getCollection())) {
      xml.append("\n    ").append(XmlHandler.addTagValue("mongo_collection", getCollection()));
    }
    if (!StringUtils.isEmpty(m_batchInsertSize)) {
      xml.append("\n    ").append(XmlHandler.addTagValue("batch_insert_size", m_batchInsertSize));
    }

    xml.append("    ").append(XmlHandler.addTagValue("connect_timeout", getConnectTimeout()));
    xml.append("    ").append(XmlHandler.addTagValue("socket_timeout", getSocketTimeout()));
    xml.append("    ")
        .append(XmlHandler.addTagValue("use_ssl_socket_factory", isUseSSLSocketFactory()));
    xml.append("    ").append(XmlHandler.addTagValue("read_preference", getReadPreference()));
    xml.append("    ").append(XmlHandler.addTagValue("write_concern", getWriteConcern()));
    xml.append("    ").append(XmlHandler.addTagValue("w_timeout", getWTimeout()));
    xml.append("    ").append(XmlHandler.addTagValue("journaled_writes", getJournal()));

    xml.append("\n    ").append(XmlHandler.addTagValue("truncate", m_truncate));
    xml.append("\n    ").append(XmlHandler.addTagValue("update", m_update));
    xml.append("\n    ").append(XmlHandler.addTagValue("upsert", m_upsert));
    xml.append("\n    ").append(XmlHandler.addTagValue("multi", m_multi));
    xml.append("\n    ").append(XmlHandler.addTagValue("modifier_update", m_modifierUpdate));

    xml.append("    ").append(XmlHandler.addTagValue("write_retries", m_writeRetries));
    xml.append("    ").append(XmlHandler.addTagValue("write_retry_delay", m_writeRetryDelay));

    if (m_mongoFields != null && m_mongoFields.size() > 0) {
      xml.append("\n    ").append(XmlHandler.openTag("mongo_fields"));

      for (MongoField field : m_mongoFields) {
        xml.append("\n      ").append(XmlHandler.openTag("mongo_field"));

        xml.append("\n         ")
            .append(XmlHandler.addTagValue("incoming_field_name", field.m_incomingFieldName));
        xml.append("\n         ")
            .append(XmlHandler.addTagValue("mongo_doc_path", field.m_mongoDocPath));
        xml.append("\n         ")
            .append(
                XmlHandler.addTagValue(
                    "use_incoming_field_name_as_mongo_field_name",
                    field.m_useIncomingFieldNameAsMongoFieldName));
        xml.append("\n         ")
            .append(XmlHandler.addTagValue("update_match_field", field.m_updateMatchField));
        xml.append("\n         ")
            .append(
                XmlHandler.addTagValue(
                    "modifier_update_operation", field.m_modifierUpdateOperation));
        xml.append("\n         ")
            .append(
                XmlHandler.addTagValue("modifier_policy", field.m_modifierOperationApplyPolicy));
        xml.append("\n         ").append(XmlHandler.addTagValue("json_field", field.m_JSON));
        xml.append("\n         ").append(XmlHandler.addTagValue("allow_null", field.insertNull));

        xml.append("\n      ").append(XmlHandler.closeTag("mongo_field"));
      }

      xml.append("\n    ").append(XmlHandler.closeTag("mongo_fields"));
    }

    if (m_mongoIndexes != null && m_mongoIndexes.size() > 0) {
      xml.append("\n    ").append(XmlHandler.openTag("mongo_indexes"));

      for (MongoIndex index : m_mongoIndexes) {
        xml.append("\n      ").append(XmlHandler.openTag("mongo_index"));

        xml.append("\n         ")
            .append(XmlHandler.addTagValue("path_to_fields", index.m_pathToFields));
        xml.append("\n         ").append(XmlHandler.addTagValue("drop", index.m_drop));
        xml.append("\n         ").append(XmlHandler.addTagValue("unique", index.m_unique));
        xml.append("\n         ").append(XmlHandler.addTagValue("sparse", index.m_sparse));

        xml.append("\n      ").append(XmlHandler.closeTag("mongo_index"));
      }

      xml.append("\n    ").append(XmlHandler.closeTag("mongo_indexes"));
    }

    return xml.toString();
  }

  @Override
  public void loadXml(Node transformnode, IHopMetadataProvider metaStore) throws HopXmlException {
    setHostnames(XmlHandler.getTagValue(transformnode, "mongo_host"));
    setPort(XmlHandler.getTagValue(transformnode, "mongo_port"));
    setAuthenticationDatabaseName(XmlHandler.getTagValue(transformnode, "mongo_auth_database"));
    setAuthenticationUser(XmlHandler.getTagValue(transformnode, "mongo_user"));
    setAuthenticationPassword(XmlHandler.getTagValue(transformnode, "mongo_password"));
    if (!StringUtils.isEmpty(getAuthenticationPassword())) {
      setAuthenticationPassword(
          Encr.decryptPasswordOptionallyEncrypted(getAuthenticationPassword()));
    }

    setAuthenticationMechanism(XmlHandler.getTagValue(transformnode, "auth_mech"));

    setUseKerberosAuthentication(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "auth_kerberos")));
    setDbName(XmlHandler.getTagValue(transformnode, "mongo_db"));
    setCollection(XmlHandler.getTagValue(transformnode, "mongo_collection"));
    m_batchInsertSize = XmlHandler.getTagValue(transformnode, "batch_insert_size");

    setConnectTimeout(XmlHandler.getTagValue(transformnode, "connect_timeout"));
    setSocketTimeout(XmlHandler.getTagValue(transformnode, "socket_timeout"));

    String useSSLSocketFactory = XmlHandler.getTagValue(transformnode, "use_ssl_socket_factory");
    if (!Utils.isEmpty(useSSLSocketFactory)) {
      setUseSSLSocketFactory(useSSLSocketFactory.equalsIgnoreCase("Y"));
    }

    setReadPreference(XmlHandler.getTagValue(transformnode, "read_preference"));
    setWriteConcern(XmlHandler.getTagValue(transformnode, "write_concern"));
    setWTimeout(XmlHandler.getTagValue(transformnode, "w_timeout"));
    String journaled = XmlHandler.getTagValue(transformnode, "journaled_writes");
    if (!StringUtils.isEmpty(journaled)) {
      setJournal(journaled.equalsIgnoreCase("Y"));
    }

    m_truncate = XmlHandler.getTagValue(transformnode, "truncate").equalsIgnoreCase("Y");

    // for backwards compatibility with older ktrs
    String update = XmlHandler.getTagValue(transformnode, "update");
    if (!StringUtils.isEmpty(update)) {
      m_update = update.equalsIgnoreCase("Y");
    }

    m_upsert = XmlHandler.getTagValue(transformnode, "upsert").equalsIgnoreCase("Y");
    m_multi = XmlHandler.getTagValue(transformnode, "multi").equalsIgnoreCase("Y");
    m_modifierUpdate = XmlHandler.getTagValue(transformnode, "modifier_update").equalsIgnoreCase("Y");

    // for backwards compatibility with older ktrs (to maintain correct
    // operation)
    if (m_upsert || m_multi) {
      m_update = true;
    }

    setUseAllReplicaSetMembers(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "use_all_replica_members")));

    String writeRetries = XmlHandler.getTagValue(transformnode, "write_retries");
    if (!StringUtils.isEmpty(writeRetries)) {
      m_writeRetries = writeRetries;
    }
    String writeRetryDelay = XmlHandler.getTagValue(transformnode, "write_retry_delay");
    if (!StringUtils.isEmpty(writeRetryDelay)) {
      m_writeRetryDelay = writeRetryDelay;
    }

    Node fields = XmlHandler.getSubNode(transformnode, "mongo_fields");
    if (fields != null && XmlHandler.countNodes(fields, "mongo_field") > 0) {
      int nrFields = XmlHandler.countNodes(fields, "mongo_field");
      m_mongoFields = new ArrayList<>();

      for (int i = 0; i < nrFields; i++) {
        Node fieldNode = XmlHandler.getSubNodeByNr(fields, "mongo_field", i);

        MongoField newField = new MongoField();
        newField.m_incomingFieldName = XmlHandler.getTagValue(fieldNode, "incoming_field_name");
        newField.m_mongoDocPath = XmlHandler.getTagValue(fieldNode, "mongo_doc_path");
        newField.m_useIncomingFieldNameAsMongoFieldName =
            XmlHandler.getTagValue(fieldNode, "use_incoming_field_name_as_mongo_field_name")
                .equalsIgnoreCase("Y");
        newField.m_updateMatchField =
            XmlHandler.getTagValue(fieldNode, "update_match_field").equalsIgnoreCase("Y");

        newField.m_modifierUpdateOperation =
            XmlHandler.getTagValue(fieldNode, "modifier_update_operation");
        String policy = XmlHandler.getTagValue(fieldNode, "modifier_policy");
        if (!StringUtils.isEmpty(policy)) {
          newField.m_modifierOperationApplyPolicy = policy;
        }
        String jsonField = XmlHandler.getTagValue(fieldNode, "json_field");
        if (!StringUtils.isEmpty(jsonField)) {
          newField.m_JSON = jsonField.equalsIgnoreCase("Y");
        }
        String allowNull = XmlHandler.getTagValue(fieldNode, "allow_null");
        newField.insertNull = "Y".equalsIgnoreCase(allowNull);

        m_mongoFields.add(newField);
      }
    }

    fields = XmlHandler.getSubNode(transformnode, "mongo_indexes");
    if (fields != null && XmlHandler.countNodes(fields, "mongo_index") > 0) {
      int nrFields = XmlHandler.countNodes(fields, "mongo_index");

      m_mongoIndexes = new ArrayList<>();

      for (int i = 0; i < nrFields; i++) {
        Node fieldNode = XmlHandler.getSubNodeByNr(fields, "mongo_index", i);

        MongoIndex newIndex = new MongoIndex();

        newIndex.m_pathToFields = XmlHandler.getTagValue(fieldNode, "path_to_fields");
        newIndex.m_drop = XmlHandler.getTagValue(fieldNode, "drop").equalsIgnoreCase("Y");
        newIndex.m_unique = XmlHandler.getTagValue(fieldNode, "unique").equalsIgnoreCase("Y");
        newIndex.m_sparse = XmlHandler.getTagValue(fieldNode, "sparse").equalsIgnoreCase("Y");

        m_mongoIndexes.add(newIndex);
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
