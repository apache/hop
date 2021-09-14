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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import org.apache.hop.core.*;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "PGBulkLoader",
    image = "PGBulkLoader.svg",
    description = "i18n::PGBulkLoader.Description",
    name = "i18n::PGBulkLoader.Name",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    documentationUrl = "/pipeline/transforms/postgresbulkloader.html")
public class PGBulkLoaderMeta extends BaseTransformMeta
    implements ITransformMeta<PGBulkLoader, PGBulkLoaderData>,
        IProvidesDatabaseConnectionInformation {

  private static final Class<?> PKG = PGBulkLoaderMeta.class; // For Translator

  /** what's the schema for the target? */
  @HopMetadataProperty(
      key = "schema",
      injectionKeyDescription = "PGBulkLoader.Injection.Schema.Label")
  private String schemaName;

  /** what's the table for the target? */
  @HopMetadataProperty(
      key = "table",
      injectionKeyDescription = "PGBulkLoader.Injection.Table.Label")
  private String tableName;

  /** database connection */
  @HopMetadataProperty(
      key = "connection",
      storeWithName = true,
      injectionKeyDescription = "PGBulkLoader.Injection.Connection.Label")
  private DatabaseMeta databaseMeta;

  /** Field value to dateMask after lookup */
  @HopMetadataProperty(
      injectionGroupKey = "mapping",
      injectionGroupDescription = "PGBulkLoader.Injection.Mapping.Label")
  private List<PGBulkLoaderMappingMeta> mapping;

  /** Load action */
  @HopMetadataProperty(
      key = "load_action",
      injectionKeyDescription = "PGBulkLoader.Injection.LoadAction.Label")
  private String loadAction;

  /** Database name override */
  @HopMetadataProperty(
      key = "db_override",
      injectionKeyDescription = "PGBulkLoader.Injection.DBOverride.Label")
  private String dbNameOverride;

  /** The field delimiter to use for loading */
  @HopMetadataProperty(
      key = "delimiter",
      injectionKeyDescription = "PGBulkLoader.Injection.Delimiter.Label")
  private String delimiter;

  /** The enclosure to use for loading */
  @HopMetadataProperty(
      key = "enclosure",
      injectionKeyDescription = "PGBulkLoader.Injection.Enclosure.Label")
  private String enclosure;

  /** Stop On Error */
  @HopMetadataProperty(
      key = "stop_on_error",
      injectionKeyDescription = "PGBulkLoader.Injection.StopOnError.Label")
  private boolean stopOnError;

  /*
   * Do not translate following values!!! They are will end up in the workflow export.
   */
  public static final String ACTION_INSERT = "INSERT";
  public static final String ACTION_TRUNCATE = "TRUNCATE";

  /*
   * Do not translate following values!!! They are will end up in the workflow export.
   */
  public static final String DATE_MASK_PASS_THROUGH = "PASS THROUGH";
  public static final String DATE_MASK_DATE = "DATE";
  public static final String DATE_MASK_DATETIME = "DATETIME";

  public static final int NR_DATE_MASK_PASS_THROUGH = 0;
  public static final int NR_DATE_MASK_DATE = 1;
  public static final int NR_DATE_MASK_DATETIME = 2;

  public PGBulkLoaderMeta() {
    super();
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

  /** @return Returns the tableName. */
  @Override
  public String getTableName() {
    return tableName;
  }

  /** @param tableName The tableName to set. */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<PGBulkLoaderMappingMeta> getMapping() {
    return mapping;
  }

  public void setMapping(List<PGBulkLoaderMappingMeta> mapping) {
    this.mapping = mapping;
  }

  @Override
  public void setDefault() {
    databaseMeta = null;
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "GPBulkLoaderMeta.DefaultTableName");
    dbNameOverride = "";
    delimiter = ";";
    enclosure = "\"";
    stopOnError = false;
    mapping = new ArrayList<>();
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Default: nothing changes to rowMeta
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
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "GPBulkLoaderMeta.CheckResult.TableNameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          // Check fields in table
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
          IRowMeta r = db.getTableFields(schemaTable);
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "GPBulkLoaderMeta.CheckResult.TableExists"),
                    transformMeta);
            remarks.add(cr);

            // How about the fields to insert/dateMask in the table?
            first = true;
            errorFound = false;
            errorMessage = "";

            for (int i = 0; i < mapping.size(); i++) {
              String field = mapping.get(i).getFieldTable();

              IValueMeta v = r.searchValueMeta(field);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "GPBulkLoaderMeta.CheckResult.MissingFieldsToLoadInTargetTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + field + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "GPBulkLoaderMeta.CheckResult.AllFieldsFoundInTargetTable"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "GPBulkLoaderMeta.CheckResult.CouldNotReadTableInfo");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG,
                      "GPBulkLoaderMeta.CheckResult.TransformReceivingDatas",
                      prev.size() + ""),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < mapping.size(); i++) {
            IValueMeta v = prev.searchValueMeta(mapping.get(i).getFieldStream());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "GPBulkLoaderMeta.CheckResult.MissingFieldsInInput")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + mapping.get(i).getFieldStream() + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "GPBulkLoaderMeta.CheckResult.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "GPBulkLoaderMeta.CheckResult.MissingFieldsInInput3")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "GPBulkLoaderMeta.CheckResult.DatabaseErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "GPBulkLoaderMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "GPBulkLoaderMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GPBulkLoaderMeta.CheckResult.NoInputError"),
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
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && prev.size() > 0) {
        // Copy the row
        IRowMeta tableFields = new RowMeta();

        // Now change the field names
        for (int i = 0; i < mapping.size(); i++) {
          IValueMeta v = prev.searchValueMeta(mapping.get(i).getFieldStream());
          if (v != null) {
            IValueMeta tableField = v.clone();
            tableField.setName(mapping.get(i).getFieldTable());
            tableFields.addValueMeta(tableField);
          } else {
            throw new HopTransformException(
                "Unable to find field [" + mapping.get(i).getFieldStream() + "] in the input rows");
          }
        }

        if (!Utils.isEmpty(tableName)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
            String sql = db.getDDL(schemaTable, tableFields, null, false, null, true);

            if (sql.length() == 0) {
              retval.setSql(null);
            } else {
              retval.setSql(sql);
            }
          } catch (HopException e) {
            retval.setError(
                BaseMessages.getString(PKG, "GPBulkLoaderMeta.GetSQL.ErrorOccurred")
                    + e.getMessage());
          }
        } else {
          retval.setError(
              BaseMessages.getString(PKG, "GPBulkLoaderMeta.GetSQL.NoTableDefinedOnConnection"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "GPBulkLoaderMeta.GetSQL.NotReceivingAnyFields"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "GPBulkLoaderMeta.GetSQL.NoConnectionDefined"));
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
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (prev != null) {
      /* DEBUG CHECK THIS */
      // Insert dateMask fields : read/write
      for (int i = 0; i < mapping.size(); i++) {
        IValueMeta v = prev.searchValueMeta(mapping.get(i).getFieldStream());

        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                variables.resolve(tableName),
                mapping.get(i).getFieldTable(),
                mapping.get(i).getFieldStream(),
                v != null ? v.getOrigin() : "?",
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }
    }
  }

  @Override
  public PGBulkLoader createTransform(
      TransformMeta transformMeta,
      PGBulkLoaderData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new PGBulkLoader(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public PGBulkLoaderData getTransformData() {
    return new PGBulkLoaderData();
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
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!Utils.isEmpty(realTableName)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, realSchemaName, realTableName);

          // Check if this table exists...
          if (db.checkTableExists(realSchemaName, realTableName)) {
            return db.getTableFields(schemaTable);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "GPBulkLoaderMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "GPBulkLoaderMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "GPBulkLoaderMeta.Exception.ErrorGettingFields"), e);
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "GPBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
  }

  /** @return the schemaName */
  @Override
  public String getSchemaName() {
    return schemaName;
  }

  /** @param schemaName the schemaName to set */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setLoadAction(String action) {
    this.loadAction = action;
  }

  public String getLoadAction() {
    return this.loadAction;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public String getEnclosure() {
    return enclosure;
  }

  public String getDbNameOverride() {
    return dbNameOverride;
  }

  public void setDbNameOverride(String dbNameOverride) {
    this.dbNameOverride = dbNameOverride;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  public void setEnclosure(String enclosure) {
    this.enclosure = enclosure;
  }

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    return null;
  }

  public boolean isStopOnError() {
    return this.stopOnError;
  }

  public void setStopOnError(Boolean value) {
    this.stopOnError = value;
  }

  public void setStopOnError(boolean value) {
    this.stopOnError = value;
  }
}
