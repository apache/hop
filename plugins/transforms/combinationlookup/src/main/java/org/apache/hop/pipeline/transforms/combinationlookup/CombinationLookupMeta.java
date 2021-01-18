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

package org.apache.hop.pipeline.transforms.combinationlookup;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProvidesModelerMeta;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/*
 * Created on 14-may-2003
 *
 * TODO: In the distant future the useAutoIncrement flag should be removed since its
 *       functionality is now taken over by techKeyCreation (which is cleaner).
 */
@Transform(
    id = "CombinationLookup",
    image = "combinationlookup.svg",
    name = "i18n::CombinationLookup.Name",
    description = "i18n::CombinationLookup.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.DataWarehouse",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/combinationlookup.html")
@InjectionSupported(localizationPrefix = "CombinationLookup.Injection.")
public class CombinationLookupMeta extends BaseTransformMeta
    implements ITransformMeta<CombinationLookup, CombinationLookupData>, IProvidesModelerMeta {

  private static final Class<?> PKG = CombinationLookupMeta.class; // For Translator

  /** Default cache size: 0 will cache everything */
  public static final int DEFAULT_CACHE_SIZE = 9999;

  /** what's the lookup schema? */
  @Injection(name = "SCHEMA_NAME")
  private String schemaName;

  /** what's the lookup table? */
  @Injection(name = "TABLE_NAME")
  private String tableName;

  /** database connection */
  private DatabaseMeta databaseMeta;

  /** replace fields with technical key? */
  @Injection(name = "REPLACE_FIELDS")
  private boolean replaceFields;

  /** which fields do we use to look up a value? */
  @Injection(name = "KEY_FIELDS")
  private String[] keyField;

  /** With which fields in dimension do we look up? */
  @Injection(name = "KEY_LOOKUP")
  private String[] keyLookup;

  /** Use checksum algorithm to limit index size? */
  @Injection(name = "USE_HASH")
  private boolean useHash;

  /** Name of the CRC field in the dimension */
  @Injection(name = "HASH_FIELD")
  private String hashField;

  /** Technical Key field to return */
  @Injection(name = "TECHNICAL_KEY_FIELD")
  private String technicalKeyField;

  /** Where to get the sequence from... */
  @Injection(name = "SEQUENCE_FROM")
  private String sequenceFrom;

  /** Commit size for insert / update */
  @Injection(name = "COMMIT_SIZE")
  private int commitSize;

  /**
   * Preload the cache, defaults to false
   *
   * @author nicow2
   */
  @Injection(name = "PRELOAD_CACHE")
  private boolean preloadCache = false;

  /** Limit the cache size to this! */
  @Injection(name = "CACHE_SIZE")
  private int cacheSize;

  /** Use the auto-increment feature of the database to generate keys. */
  @Injection(name = "AUTO_INC")
  private boolean useAutoinc;

  /** Which method to use for the creation of the tech key */
  @Injection(name = "TECHNICAL_KEY_CREATION")
  private String techKeyCreation = null;

  @Injection(name = "LAST_UPDATE_FIELD")
  private String lastUpdateField;

  public static String CREATION_METHOD_AUTOINC = "autoinc";
  public static String CREATION_METHOD_SEQUENCE = "sequence";
  public static String CREATION_METHOD_TABLEMAX = "tablemax";

  @Injection(name = "CONNECTIONNAME")
  public void setConnection(String connectionName) {
    databaseMeta =
        DatabaseMeta.findDatabase(
            getParentTransformMeta().getParentPipelineMeta().getDatabases(), connectionName);
  }

  /** @return Returns the database. */
  @Override
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param database The database to set. */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /**
   * Set the way how the technical key field should be created.
   *
   * @param techKeyCreation which method to use for the creation of the technical key.
   */
  public void setTechKeyCreation(String techKeyCreation) {
    this.techKeyCreation = techKeyCreation;
  }

  /**
   * Get the way how the technical key field should be created.
   *
   * @return creation way for the technical key.
   */
  public String getTechKeyCreation() {
    return this.techKeyCreation;
  }

  /** @return Returns the commitSize. */
  public int getCommitSize() {
    return commitSize;
  }

  /** @param commitSize The commitSize to set. */
  public void setCommitSize(int commitSize) {
    this.commitSize = commitSize;
  }

  /** @return Returns the cacheSize. */
  public int getCacheSize() {
    return cacheSize;
  }

  /** @param cacheSize The cacheSize to set. */
  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  /** @return Returns the hashField. */
  public String getHashField() {
    return hashField;
  }

  /** @param hashField The hashField to set. */
  public void setHashField(String hashField) {
    this.hashField = hashField;
  }

  /** @return Returns the keyField (names in the stream). */
  public String[] getKeyField() {
    return keyField;
  }

  /** @param keyField The keyField to set. */
  public void setKeyField(String[] keyField) {
    this.keyField = keyField;
  }

  /** @return Returns the keyLookup (names in the dimension table) */
  public String[] getKeyLookup() {
    return keyLookup;
  }

  /** @param keyLookup The keyLookup to set. */
  public void setKeyLookup(String[] keyLookup) {
    this.keyLookup = keyLookup;
  }

  /** @return Returns the replaceFields. */
  public boolean replaceFields() {
    return replaceFields;
  }

  /** @param replaceFields The replaceFields to set. */
  public void setReplaceFields(boolean replaceFields) {
    this.replaceFields = replaceFields;
  }

  /** @param preloadCache true to preload the cache */
  public void setPreloadCache(boolean preloadCache) {
    this.preloadCache = preloadCache;
  }

  /** @return Returns true if preload the cache. */
  public boolean getPreloadCache() {
    return preloadCache;
  }

  /** @return Returns the sequenceFrom. */
  public String getSequenceFrom() {
    return sequenceFrom;
  }

  /** @param sequenceFrom The sequenceFrom to set. */
  public void setSequenceFrom(String sequenceFrom) {
    this.sequenceFrom = sequenceFrom;
  }

  /** @return Returns the tablename. */
  @Override
  public String getTableName() {
    return tableName;
  }

  /** @param tableName The tablename to set. */
  public void setTablename(String tableName) {
    this.tableName = tableName;
  }

  /** @return Returns the technicalKeyField. */
  public String getTechnicalKeyField() {
    return technicalKeyField;
  }

  /** @param technicalKeyField The technicalKeyField to set. */
  public void setTechnicalKeyField(String technicalKeyField) {
    this.technicalKeyField = technicalKeyField;
  }

  /** @return Returns the useAutoinc. */
  public boolean isUseAutoinc() {
    return useAutoinc;
  }

  /** @param useAutoinc The useAutoinc to set. */
  public void setUseAutoinc(boolean useAutoinc) {
    this.useAutoinc = useAutoinc;
  }

  /** @return Returns the useHash. */
  public boolean useHash() {
    return useHash;
  }

  /** @param useHash The useHash to set. */
  public void setUseHash(boolean useHash) {
    this.useHash = useHash;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrkeys) {
    keyField = new String[nrkeys];
    keyLookup = new String[nrkeys];
  }

  @Override
  public Object clone() {
    CombinationLookupMeta retval = (CombinationLookupMeta) super.clone();

    int nrkeys = keyField.length;

    retval.allocate(nrkeys);
    System.arraycopy(keyField, 0, retval.keyField, 0, nrkeys);
    System.arraycopy(keyLookup, 0, retval.keyLookup, 0, nrkeys);

    return retval;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    this.databases = databases;
    try {
      String commit, csize;

      schemaName = XmlHandler.getTagValue(transformNode, "schema");
      tableName = XmlHandler.getTagValue(transformNode, "table");
      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      commit = XmlHandler.getTagValue(transformNode, "commit");
      commitSize = Const.toInt(commit, 0);
      csize = XmlHandler.getTagValue(transformNode, "cache_size");
      cacheSize = Const.toInt(csize, 0);

      replaceFields = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "replace"));
      preloadCache = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "preloadCache"));
      useHash = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "crc"));

      hashField = XmlHandler.getTagValue(transformNode, "crcfield");

      Node keys = XmlHandler.getSubNode(transformNode, "fields");
      int nrkeys = XmlHandler.countNodes(keys, "key");

      allocate(nrkeys);

      // Read keys to dimension
      for (int i = 0; i < nrkeys; i++) {
        Node knode = XmlHandler.getSubNodeByNr(keys, "key", i);
        keyField[i] = XmlHandler.getTagValue(knode, "name");
        keyLookup[i] = XmlHandler.getTagValue(knode, "lookup");
      }

      // If this is empty: use auto-increment field!
      sequenceFrom = XmlHandler.getTagValue(transformNode, "sequence");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      Node retkey = XmlHandler.getSubNode(fields, "return");
      technicalKeyField = XmlHandler.getTagValue(retkey, "name");
      useAutoinc = !"N".equalsIgnoreCase(XmlHandler.getTagValue(retkey, "use_autoinc"));
      lastUpdateField = XmlHandler.getTagValue(transformNode, "last_update_field");

      setTechKeyCreation(XmlHandler.getTagValue(retkey, "creation_method"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "CombinationLookupMeta.Exception.UnableToLoadTransformMeta"),
          e);
    }
  }

  @Override
  public void setDefault() {
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "CombinationLookupMeta.DimensionTableName.Label");
    databaseMeta = null;
    commitSize = 100;
    cacheSize = DEFAULT_CACHE_SIZE;
    replaceFields = false;
    preloadCache = false;
    useHash = false;
    hashField = "hashcode";
    int nrkeys = 0;

    allocate(nrkeys);

    // Read keys to dimension
    for (int i = 0; i < nrkeys; i++) {
      keyField[i] = "key" + i;
      keyLookup[i] = "keylookup" + i;
    }

    technicalKeyField = "technical/surrogate key field";
    useAutoinc = false;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    IValueMeta v = new ValueMetaInteger(technicalKeyField);
    v.setLength(10);
    v.setPrecision(0);
    v.setOrigin(origin);
    row.addValueMeta(v);

    if (replaceFields) {
      for (int i = 0; i < keyField.length; i++) {
        int idx = row.indexOfValue(keyField[i]);
        if (idx >= 0) {
          row.removeValueMeta(idx);
        }
      }
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(512);

    retval.append("      ").append(XmlHandler.addTagValue("schema", schemaName));
    retval.append("      ").append(XmlHandler.addTagValue("table", tableName));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("      ").append(XmlHandler.addTagValue("commit", commitSize));
    retval.append("      ").append(XmlHandler.addTagValue("cache_size", cacheSize));
    retval.append("      ").append(XmlHandler.addTagValue("replace", replaceFields));
    retval.append("      ").append(XmlHandler.addTagValue("preloadCache", preloadCache));
    retval.append("      ").append(XmlHandler.addTagValue("crc", useHash));
    retval.append("      ").append(XmlHandler.addTagValue("crcfield", hashField));

    retval.append("      <fields>").append(Const.CR);
    for (int i = 0; i < keyField.length; i++) {
      retval.append("        <key>").append(Const.CR);
      retval.append("          ").append(XmlHandler.addTagValue("name", keyField[i]));
      retval.append("          ").append(XmlHandler.addTagValue("lookup", keyLookup[i]));
      retval.append("        </key>").append(Const.CR);
    }

    retval.append("        <return>").append(Const.CR);
    retval.append("          ").append(XmlHandler.addTagValue("name", technicalKeyField));
    retval.append("          ").append(XmlHandler.addTagValue("creation_method", techKeyCreation));
    retval.append("          ").append(XmlHandler.addTagValue("use_autoinc", useAutoinc));
    retval.append("        </return>").append(Const.CR);

    retval.append("      </fields>").append(Const.CR);

    // If sequence is empty: use auto-increment field!
    retval.append("      ").append(XmlHandler.addTagValue("sequence", sequenceFrom));
    retval.append("      ").append(XmlHandler.addTagValue("last_update_field", lastUpdateField));

    return retval.toString();
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
    String errorMessage = "";

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta );
      try {
        db.connect();

        if (!Utils.isEmpty(tableName)) {
          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
          IRowMeta r = db.getTableFields(schemaTable);
          if (r != null) {
            for (int i = 0; i < keyLookup.length; i++) {
              String lufield = keyLookup[i];

              IValueMeta v = r.searchValueMeta(lufield);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "CombinationLookupMeta.CheckResult.MissingCompareFields")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + lufield + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "CombinationLookupMeta.CheckResult.AllFieldsFound"),
                      transformMeta);
            }
            remarks.add(cr);

            /* Also, check the fields: tk, version, from-to, ... */
            if (r.indexOfValue(technicalKeyField) < 0) {
              errorMessage =
                  BaseMessages.getString(
                          PKG,
                          "CombinationLookupMeta.CheckResult.TechnicalKeyNotFound",
                          technicalKeyField)
                      + Const.CR;
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              errorMessage =
                  BaseMessages.getString(
                          PKG,
                          "CombinationLookupMeta.CheckResult.TechnicalKeyFound",
                          technicalKeyField)
                      + Const.CR;
              cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(
                    PKG, "CombinationLookupMeta.CheckResult.CouldNotReadTableInfo");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < keyField.length; i++) {
            IValueMeta v = prev.searchValueMeta(keyField[i]);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "CombinationLookupMeta.CheckResult.MissingFields")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + keyField[i] + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "CombinationLookupMeta.CheckResult.AllFieldsFoundInInputStream"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "CombinationLookupMeta.CheckResult.CouldNotReadFields")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }

        // Check sequence
        if (databaseMeta.supportsSequences()
            && CREATION_METHOD_SEQUENCE.equals(getTechKeyCreation())) {
          if (Utils.isEmpty(sequenceFrom)) {
            errorMessage +=
                BaseMessages.getString(PKG, "CombinationLookupMeta.CheckResult.ErrorNoSequenceName")
                    + "!";
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          } else {
            // It doesn't make sense to check the sequence name
            // if it's not filled in.
            if (db.checkSequenceExists(sequenceFrom)) {
              errorMessage =
                  BaseMessages.getString(
                      PKG, "CombinationLookupMeta.CheckResult.ReadingSequenceOK", sequenceFrom);
              cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
              remarks.add(cr);
            } else {
              errorMessage +=
                  BaseMessages.getString(
                          PKG, "CombinationLookupMeta.CheckResult.ErrorReadingSequence")
                      + sequenceFrom
                      + "!";
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
              remarks.add(cr);
            }
          }
        }

        if (techKeyCreation != null) {
          // post 2.2 version
          if (!(CREATION_METHOD_AUTOINC.equals(techKeyCreation)
              || CREATION_METHOD_SEQUENCE.equals(techKeyCreation)
              || CREATION_METHOD_TABLEMAX.equals(techKeyCreation))) {
            errorMessage +=
                BaseMessages.getString(
                        PKG, "CombinationLookupMeta.CheckResult.ErrorTechKeyCreation")
                    + ": "
                    + techKeyCreation
                    + "!";
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "CombinationLookupMeta.CheckResult.ErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "CombinationLookupMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CombinationLookupMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "CombinationLookupMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IHopMetadataProvider metadataProvider) {
    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    int i;

    if (databaseMeta != null) {
      if (prev != null && prev.size() > 0) {
        if (!Utils.isEmpty(tableName)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
          Database db = new Database(loggingObject, variables, databaseMeta );
          try {
            boolean doHash = false;
            String crTable = null;

            db.connect();

            // OK, what do we put in the new table??
            IRowMeta fields = new RowMeta();

            // First, the new technical key...
            IValueMeta vkeyfield = new ValueMetaInteger(technicalKeyField);
            vkeyfield.setLength(10);
            vkeyfield.setPrecision(0);

            // Then the hashcode (optional)
            IValueMeta vhashfield = null;
            if (useHash && !Utils.isEmpty(hashField)) {
              vhashfield = new ValueMetaInteger(hashField);
              vhashfield.setLength(15);
              vhashfield.setPrecision(0);
              doHash = true;
            }

            // Then the last update field (optional)
            IValueMeta vLastUpdateField = null;
            if (!Utils.isEmpty(lastUpdateField)) {
              vLastUpdateField = new ValueMetaDate(lastUpdateField);
            }

            if (!db.checkTableExists(schemaTable)) {
              // Add technical key field.
              fields.addValueMeta(vkeyfield);

              // Add the keys only to the table
              if (keyField != null && keyLookup != null) {
                int cnt = keyField.length;
                for (i = 0; i < cnt; i++) {
                  String errorField = "";

                  // Find the value in the stream
                  IValueMeta v = prev.searchValueMeta(keyField[i]);
                  if (v != null) {
                    String name = keyLookup[i];
                    IValueMeta newValue = v.clone();
                    newValue.setName(name);

                    if (name.equals(vkeyfield.getName())
                        || (doHash == true && name.equals(vhashfield.getName()))) {
                      errorField += name;
                    }
                    if (errorField.length() > 0) {
                      retval.setError(
                          BaseMessages.getString(
                              PKG, "CombinationLookupMeta.ReturnValue.NameCollision", errorField));
                    } else {
                      fields.addValueMeta(newValue);
                    }
                  }
                }
              }

              if (doHash == true) {
                fields.addValueMeta(vhashfield);
              }

              if (vLastUpdateField != null) {
                fields.addValueMeta(vLastUpdateField);
              }
            } else {
              // Table already exists

              // Get the fields that are in the table now:
              IRowMeta tabFields = db.getTableFields(schemaTable);

              // Don't forget to quote these as well...
              databaseMeta.quoteReservedWords(tabFields);

              if (tabFields.searchValueMeta(vkeyfield.getName()) == null) {
                // Add technical key field if it didn't exist yet
                fields.addValueMeta(vkeyfield);
              }

              // Add the already existing fields
              int cnt = tabFields.size();
              for (i = 0; i < cnt; i++) {
                IValueMeta v = tabFields.getValueMeta(i);

                fields.addValueMeta(v);
              }

              // Find the missing fields in the real table
              String[] keyLookup = getKeyLookup();
              String[] keyField = getKeyField();
              if (keyField != null && keyLookup != null) {
                cnt = keyField.length;
                for (i = 0; i < cnt; i++) {
                  // Find the value in the stream
                  IValueMeta v = prev.searchValueMeta(keyField[i]);
                  if (v != null) {
                    IValueMeta newValue = v.clone();
                    newValue.setName(keyLookup[i]);

                    // Does the corresponding name exist in the table
                    if (tabFields.searchValueMeta(newValue.getName()) == null) {
                      fields.addValueMeta(newValue); // nope --> add
                    }
                  }
                }
              }

              if (doHash == true && tabFields.searchValueMeta(vhashfield.getName()) == null) {
                // Add hash field
                fields.addValueMeta(vhashfield);
              }

              if (vLastUpdateField != null
                  && tabFields.searchValueMeta(vLastUpdateField.getName()) == null) {
                fields.addValueMeta(vLastUpdateField);
              }
            }

            crTable =
                db.getDDL(
                    schemaTable,
                    fields,
                    (CREATION_METHOD_SEQUENCE.equals(getTechKeyCreation())
                            && sequenceFrom != null
                            && sequenceFrom.length() != 0)
                        ? null
                        : technicalKeyField,
                    CREATION_METHOD_AUTOINC.equals(getTechKeyCreation()),
                    null,
                    true);

            //
            // OK, now let's build the index
            //

            // What fields do we put int the index?
            // Only the hashcode or all fields?
            String crIndex = "";
            String cr_uniqIndex = "";
            String[] idxFields = null;
            if (useHash) {
              if (hashField != null && hashField.length() > 0) {
                idxFields = new String[] {hashField};
              } else {
                retval.setError(
                    BaseMessages.getString(
                        PKG, "CombinationLookupMeta.ReturnValue.NotHashFieldSpecified"));
              }
            } else {
              // index on all key fields...
              if (!Utils.isEmpty(keyLookup)) {
                int nrFields = keyLookup.length;
                int maxFields = databaseMeta.getMaxColumnsInIndex();
                if (maxFields > 0 && nrFields > maxFields) {
                  nrFields = maxFields; // For example, oracle indexes are limited to 32 fields...
                }
                idxFields = new String[nrFields];
                for (i = 0; i < nrFields; i++) {
                  idxFields[i] = keyLookup[i];
                }
              } else {
                retval.setError(
                    BaseMessages.getString(
                        PKG, "CombinationLookupMeta.ReturnValue.NotFieldsSpecified"));
              }
            }

            // OK, now get the create index statement...

            if (!Utils.isEmpty(technicalKeyField)) {
              String[] techKeyArr = new String[] {technicalKeyField};
              if (!db.checkIndexExists(schemaTable, techKeyArr)) {
                String indexname = "idx_" + tableName + "_pk";
                cr_uniqIndex =
                    db.getCreateIndexStatement(
                        schemaTable, indexname, techKeyArr, true, true, false, true);
                cr_uniqIndex += Const.CR;
              }
            }

            // OK, now get the create lookup index statement...
            if (!Utils.isEmpty(idxFields) && !db.checkIndexExists(schemaTable, idxFields)) {
              String indexname = "idx_" + tableName + "_lookup";
              crIndex =
                  db.getCreateIndexStatement(
                      schemaTable, indexname, idxFields, false, false, false, true);
              crIndex += Const.CR;
            }

            //
            // Don't forget the sequence (optional)
            //
            String crSeq = "";
            if (databaseMeta.supportsSequences() && !Utils.isEmpty(sequenceFrom)) {
              if (!db.checkSequenceExists(schemaName, sequenceFrom)) {
                crSeq += db.getCreateSequenceStatement(schemaName, sequenceFrom, 1L, 1L, -1L, true);
                crSeq += Const.CR;
              }
            }
            retval.setSql(variables.resolve(crTable + cr_uniqIndex + crIndex + crSeq));
          } catch (HopException e) {
            retval.setError(
                BaseMessages.getString(PKG, "CombinationLookupMeta.ReturnValue.ErrorOccurred")
                    + Const.CR
                    + e.getMessage());
          }
        } else {
          retval.setError(
              BaseMessages.getString(PKG, "CombinationLookupMeta.ReturnValue.NotTableDefined"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "CombinationLookupMeta.ReturnValue.NotReceivingField"));
      }
    } else {
      retval.setError(
          BaseMessages.getString(PKG, "CombinationLookupMeta.ReturnValue.NotConnectionDefined"));
    }

    return retval;
  }

  @Override
  public CombinationLookup createTransform(
      TransformMeta transformMeta,
      CombinationLookupData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new CombinationLookup(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public CombinationLookupData getTransformData() {
    return new CombinationLookupData();
  }

  @Override
  public void analyseImpact(
      IVariables variables,
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IHopMetadataProvider metadataProvider) {
    // The keys are read-only...
    for (int i = 0; i < keyField.length; i++) {
      IValueMeta v = prev.searchValueMeta(keyField[i]);
      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ_WRITE,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              keyLookup[i],
              keyField[i],
              v != null ? v.getOrigin() : "?",
              "",
              useHash
                  ? BaseMessages.getString(PKG, "CombinationLookupMeta.ReadAndInsert.Label")
                  : BaseMessages.getString(PKG, "CombinationLookupMeta.LookupAndInsert.Label"));
      impact.add(ii);
    }

    // Do we lookup-on the hash-field?
    if (useHash) {
      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ_WRITE,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              hashField,
              "",
              "",
              "",
              BaseMessages.getString(PKG, "CombinationLookupMeta.KeyLookup.Label"));
      impact.add(ii);
    }
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (getClass() != other.getClass()) {
      return false;
    }
    CombinationLookupMeta o = (CombinationLookupMeta) other;

    if (getCommitSize() != o.getCommitSize()) {
      return false;
    }
    if (getCacheSize() != o.getCacheSize()) {
      return false;
    }
    if (!getTechKeyCreation().equals(o.getTechKeyCreation())) {
      return false;
    }
    if (replaceFields() != o.replaceFields()) {
      return false;
    }
    if (useHash() != o.useHash()) {
      return false;
    }
    if (getPreloadCache() != o.getPreloadCache()) {
      return false;
    }
    if ((getSequenceFrom() == null && o.getSequenceFrom() != null)
        || (getSequenceFrom() != null && o.getSequenceFrom() == null)
        || (getSequenceFrom() != null
            && o.getSequenceFrom() != null
            && !getSequenceFrom().equals(o.getSequenceFrom()))) {
      return false;
    }

    if ((getSchemaName() == null && o.getSchemaName() != null)
        || (getSchemaName() != null && o.getSchemaName() == null)
        || (getSchemaName() != null
            && o.getSchemaName() != null
            && !getSchemaName().equals(o.getSchemaName()))) {
      return false;
    }

    if ((getTableName() == null && o.getTableName() != null)
        || (getTableName() != null && o.getTableName() == null)
        || (getTableName() != null
            && o.getTableName() != null
            && !getTableName().equals(o.getTableName()))) {
      return false;
    }

    if ((getHashField() == null && o.getHashField() != null)
        || (getHashField() != null && o.getHashField() == null)
        || (getHashField() != null
            && o.getHashField() != null
            && !getHashField().equals(o.getHashField()))) {
      return false;
    }

    if ((getTechnicalKeyField() == null && o.getTechnicalKeyField() != null)
        || (getTechnicalKeyField() != null && o.getTechnicalKeyField() == null)
        || (getTechnicalKeyField() != null
            && o.getTechnicalKeyField() != null
            && !getTechnicalKeyField().equals(o.getTechnicalKeyField()))) {
      return false;
    }

    // comparison missing for the following, but can be added later
    // if required.
    // getKeyField()
    // getKeyLookup()

    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getCommitSize(),
        getCacheSize(),
        getTechKeyCreation(),
        replaceFields(),
        useHash(),
        getPreloadCache(),
        getSequenceFrom(),
        getSchemaName(),
        getTableName(),
        getHashField(),
        getTechnicalKeyField());
  }

  /** @return the schemaName */
  @Override
  public String getSchemaName() {
    return schemaName;
  }

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    return null;
  }

  /** @param schemaName the schemaName to set */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /** @return the lastUpdateField */
  public String getLastUpdateField() {
    return lastUpdateField;
  }

  /** @param lastUpdateField the lastUpdateField to set */
  public void setLastUpdateField(String lastUpdateField) {
    this.lastUpdateField = lastUpdateField;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  protected IRowMeta getDatabaseTableFields(Database db, String schemaName, String tableName)
      throws HopDatabaseException {
    // First try without connecting to the database... (can be S L O W)
    String schemaTable = databaseMeta.getQuotedSchemaTableCombination(db, schemaName, tableName);
    IRowMeta extraFields = db.getTableFields(schemaTable);
    if (extraFields == null) { // now we need to connect
      db.connect();
      extraFields = db.getTableFields(schemaTable);
    }
    return extraFields;
  }

  Database createDatabaseObject(IVariables variables) {
    return new Database(loggingObject, variables, databaseMeta );
  }

  @Override
  public RowMeta getRowMeta(IVariables variables, ITransformData transformData) {
    try {
      return (RowMeta) getDatabaseTableFields(createDatabaseObject(variables), schemaName, getTableName());
    } catch (HopDatabaseException e) {
      log.logError("", e);
      return new RowMeta();
    }
  }

  @Override
  public List<String> getDatabaseFields() {
    return Arrays.asList(keyLookup);
  }

  @Override
  public List<String> getStreamFields() {
    return Arrays.asList(keyField);
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = (keyField == null) ? -1 : keyField.length;
    if (nrFields <= 0) {
      return;
    }
    String[][] rtn = Utils.normalizeArrays(nrFields, keyLookup);
    keyLookup = rtn[0];
  }
}
