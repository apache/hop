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

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "CombinationLookup",
    image = "combinationlookup.svg",
    name = "i18n::CombinationLookup.Name",
    description = "i18n::CombinationLookup.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.DataWarehouse",
    keywords = "i18n::CombinationLookupMeta.keyword",
    documentationUrl = "/pipeline/transforms/combinationlookup.html",
    actionTransformTypes = {ActionTransformType.RDBMS, ActionTransformType.LOOKUP})
@Getter
@Setter
public class CombinationLookupMeta
    extends BaseTransformMeta<CombinationLookup, CombinationLookupData> {

  private static final Class<?> PKG = CombinationLookupMeta.class;

  /** Default cache size: 0 will cache everything */
  public static final int DEFAULT_CACHE_SIZE = 9999;

  /** what's the lookup schema? */
  @HopMetadataProperty(
      key = "schema",
      injectionKey = "SCHEMA_NAME",
      injectionKeyDescription = "CombinationLookup.Injection.SCHEMA_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  /** what's the lookup table? */
  @HopMetadataProperty(
      key = "table",
      injectionKey = "TABLE_NAME",
      injectionKeyDescription = "CombinationLookup.Injection.TABLE_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  /** database connection */
  @HopMetadataProperty(
      key = "connection",
      storeWithName = true,
      injectionKey = "CONNECTIONNAME",
      injectionKeyDescription = "CombinationLookup.Injection.CONNECTION_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private DatabaseMeta databaseMeta;

  /** replace fields with technical key? */
  @HopMetadataProperty(
      key = "replace",
      injectionKey = "REPLACE_FIELDS",
      injectionKeyDescription = "CombinationLookup.Injection.REPLACE_FIELDS")
  private boolean replaceFields;

  /** Use checksum algorithm to limit index size? */
  @HopMetadataProperty(
      key = "crc",
      injectionKey = "USE_HASH",
      injectionKeyDescription = "CombinationLookup.Injection.USE_HASH")
  private boolean useHash;

  /** Name of the CRC field in the dimension */
  @HopMetadataProperty(
      key = "crcfield",
      injectionKey = "HASH_FIELD",
      injectionKeyDescription = "CombinationLookup.Injection.HASH_FIELD")
  private String hashField;

  /** Commit size for insert / update */
  @HopMetadataProperty(
      key = "commit",
      injectionKey = "COMMIT_SIZE",
      injectionKeyDescription = "CombinationLookup.Injection.COMMIT_SIZE")
  private int commitSize;

  /** Preload the cache, defaults to false */
  @HopMetadataProperty(
      key = "preloadCache",
      injectionKey = "PRELOAD_CACHE",
      injectionKeyDescription = "CombinationLookup.Injection.PRELOAD_CACHE")
  private boolean preloadCache = false;

  /** Limit the cache size to this! */
  @HopMetadataProperty(
      key = "cache_size",
      injectionKey = "CACHE_SIZE",
      injectionKeyDescription = "CombinationLookup.Injection.CACHE_SIZE")
  private int cacheSize;

  @HopMetadataProperty private CFields fields;

  public static final String CREATION_METHOD_AUTOINC = "autoinc";
  public static final String CREATION_METHOD_SEQUENCE = "sequence";
  public static final String CREATION_METHOD_TABLEMAX = "tablemax";

  public CombinationLookupMeta() {
    this.fields = new CFields();
  }

  public CombinationLookupMeta(CombinationLookupMeta m) {
    fields = new CFields();
  }

  @Override
  public Object clone() {
    CombinationLookupMeta retval = (CombinationLookupMeta) super.clone();
    return retval;
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
    fields = new CFields();
    fields.getReturnFields().setTechnicalKeyField("technical/surrogate key field");
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
    IValueMeta v = new ValueMetaInteger(fields.getReturnFields().getTechnicalKeyField());
    v.setLength(10);
    v.setPrecision(0);
    v.setOrigin(origin);
    row.addValueMeta(v);

    if (replaceFields) {
      for (int i = 0; i < fields.getKeyFields().size(); i++) {
        KeyField keyField = fields.getKeyFields().get(i);
        int idx = row.indexOfValue(keyField.getName());
        if (idx >= 0) {
          row.removeValueMeta(idx);
        }
      }
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
    String errorMessage = "";

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
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
            for (int i = 0; i < fields.getKeyFields().size(); i++) {
              KeyField keyField = fields.getKeyFields().get(i);
              String lookupField = keyField.getLookup();

              IValueMeta v = r.searchValueMeta(lookupField);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "CombinationLookupMeta.CheckResult.MissingCompareFields")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + lookupField + Const.CR;
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

            String technicalKeyField = fields.getReturnFields().getTechnicalKeyField();

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

          for (int i = 0; i < fields.getKeyFields().size(); i++) {
            KeyField keyField = fields.getKeyFields().get(i);
            IValueMeta v = prev.searchValueMeta(keyField.getName());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "CombinationLookupMeta.CheckResult.MissingFields")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + keyField.getName() + Const.CR;
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

        String techKeyCreation = fields.getReturnFields().getTechKeyCreation();
        String sequenceFrom = fields.getSequenceFrom();

        // Check sequence
        if (databaseMeta.supportsSequences() && CREATION_METHOD_SEQUENCE.equals(techKeyCreation)) {
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
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            boolean doHash = false;
            String crTable = null;

            db.connect();

            // OK, what do we put in the new table??
            IRowMeta fields = new RowMeta();

            String technicalKeyField = this.fields.getReturnFields().getTechnicalKeyField();
            String sequenceFrom = this.fields.getSequenceFrom();
            String lastUpdateField = this.fields.getReturnFields().getLastUpdateField();

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

            if (!db.checkTableExists(variables.resolve(schemaName), variables.resolve(tableName))) {
              // Add technical key field.
              fields.addValueMeta(vkeyfield);

              // Add the keys only to the table
              int cnt = this.fields.getKeyFields().size();
              for (i = 0; i < cnt; i++) {
                KeyField keyField = this.fields.getKeyFields().get(i);
                String errorField = "";

                // Find the value in the stream
                IValueMeta v = prev.searchValueMeta(keyField.getName());
                if (v != null) {
                  String name = keyField.getLookup();
                  IValueMeta newValue = v.clone();
                  newValue.setName(name);

                  if (name.equals(vkeyfield.getName())
                      || (doHash && name.equals(vhashfield.getName()))) {
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

              if (doHash) {
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

              for (i = 0; i < this.fields.getKeyFields().size(); i++) {
                KeyField keyField = this.fields.getKeyFields().get(i);
                // Find the value in the stream
                IValueMeta v = prev.searchValueMeta(keyField.getName());
                if (v != null) {
                  IValueMeta newValue = v.clone();
                  newValue.setName(keyField.getLookup());

                  // Does the corresponding name exist in the table
                  if (tabFields.searchValueMeta(newValue.getName()) == null) {
                    fields.addValueMeta(newValue); // nope --> add
                  }
                }
              }

              if (doHash && tabFields.searchValueMeta(vhashfield.getName()) == null) {
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
                    (CREATION_METHOD_SEQUENCE.equals(technicalKeyField)
                            && sequenceFrom != null
                            && sequenceFrom.length() != 0)
                        ? null
                        : technicalKeyField,
                    CREATION_METHOD_AUTOINC.equals(technicalKeyField),
                    null,
                    true);

            //
            // OK, now let's build the index
            //

            // What fields do we put int the index?
            // Only the hashcode or all fields?
            String crIndex = "";
            String crUniqIndex = "";
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
              if (!this.fields.getKeyFields().isEmpty()) {
                int nrFields = this.fields.getKeyFields().size();
                int maxFields = databaseMeta.getMaxColumnsInIndex();
                if (maxFields > 0 && nrFields > maxFields) {
                  nrFields = maxFields; // For example, oracle indexes are limited to 32 fields...
                }
                idxFields = new String[nrFields];
                for (i = 0; i < nrFields; i++) {
                  KeyField keyField = this.fields.getKeyFields().get(i);
                  idxFields[i] = keyField.getLookup();
                }
              } else {
                retval.setError(
                    BaseMessages.getString(
                        PKG, "CombinationLookupMeta.ReturnValue.NotFieldsSpecified"));
              }
            }

            // OK, now get the create index statement...
            //
            if (!Utils.isEmpty(technicalKeyField)) {
              String[] techKeyArr = new String[] {technicalKeyField};
              if (!db.checkIndexExists(schemaTable, techKeyArr)) {
                String indexname = "idx_" + tableName + "_pk";
                crUniqIndex =
                    db.getCreateIndexStatement(
                        schemaTable, indexname, techKeyArr, true, true, false, true);
                crUniqIndex += Const.CR;
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
            retval.setSql(variables.resolve(crTable + crUniqIndex + crIndex + crSeq));
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
    for (int i = 0; i < fields.getKeyFields().size(); i++) {
      KeyField keyField = fields.getKeyFields().get(i);
      IValueMeta v = prev.searchValueMeta(keyField.getName());
      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ_WRITE,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              keyField.getLookup(),
              keyField.getName(),
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
}
