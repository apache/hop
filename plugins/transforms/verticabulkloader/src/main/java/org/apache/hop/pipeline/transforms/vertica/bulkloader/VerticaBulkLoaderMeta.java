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

package org.apache.hop.pipeline.transforms.vertica.bulkloader;

import org.apache.hop.core.*;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Transform(
    id = "VerticaBulkLoader",
    image = "vertica.svg",
    name = "i18n::BaseTransform.TypeLongDesc.VerticaBulkLoaderMessage",
    description = "i18n::BaseTransform.TypeTooltipDesc.VerticaBulkLoaderMessage",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/verticabulkloader.html")
@InjectionSupported(
    localizationPrefix = "VerticaBulkLoader.Injection.",
    groups = {"FIELDS", "MAIN_OPTIONS", "DATABASE_FIELDS"})
public class VerticaBulkLoaderMeta
    extends BaseTransformMeta<VerticaBulkLoader, VerticaBulkLoaderData>
    implements IProvidesModelerMeta {
  private static final Class<?> PKG = VerticaBulkLoaderMeta.class;

  private DatabaseMeta databaseMeta;

  @Injection(name = "SCHEMANAME", group = "FIELDS")
  private String schemaName;

  @Injection(name = "TABLENAME", group = "FIELDS")
  private String tablename;

  @Injection(name = "DIRECT", group = "MAIN_OPTIONS")
  private boolean direct = true;

  @Injection(name = "ABORTONERROR", group = "MAIN_OPTIONS")
  private boolean abortOnError = true;

  @Injection(name = "EXCEPTIONSFILENAME", group = "MAIN_OPTIONS")
  private String exceptionsFileName;

  @Injection(name = "REJECTEDDATAFILENAME", group = "MAIN_OPTIONS")
  private String rejectedDataFileName;

  @Injection(name = "STREAMNAME", group = "MAIN_OPTIONS")
  private String streamName;

  /** Do we explicitly select the fields to update in the database */
  private boolean specifyFields;

  @Injection(name = "FIELDSTREAM", group = "DATABASE_FIELDS")
  /** Fields containing the values in the input stream to insert */
  private String[] fieldStream;

  @Injection(name = "FIELDDATABASE", group = "DATABASE_FIELDS")
  /** Fields in the table to insert */
  private String[] fieldDatabase;

  /*
      @Injection( name = "CONNECTIONNAME" )
      public void setConnection( String connectionName ) {
          databaseMeta = DatabaseMeta.loadDatabase(metadat, connectionName );
      }
  */

  public VerticaBulkLoaderMeta() {
    super(); // allocate BaseTransformMeta

    fieldStream = new String[0];
    fieldDatabase = new String[0];
  }

  public void allocate(int nrRows) {
    fieldStream = new String[nrRows];
    fieldDatabase = new String[nrRows];
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    VerticaBulkLoaderMeta retval = (VerticaBulkLoaderMeta) super.clone();

    int nrStream = fieldStream.length;
    int nrDatabase = fieldDatabase.length;

    retval.fieldStream = new String[nrStream];
    retval.fieldDatabase = new String[nrDatabase];

    for (int i = 0; i < nrStream; i++) {
      retval.fieldStream[i] = fieldStream[i];
    }

    for (int i = 0; i < nrDatabase; i++) {
      retval.fieldDatabase[i] = fieldDatabase[i];
    }

    return retval;
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param database The database to set.
   */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /**
   * @deprecated use {@link #getTableName()}
   */
  public String getTablename() {
    return getTableName();
  }

  /**
   * @return Returns the tablename.
   */
  public String getTableName() {
    return tablename;
  }

  /**
   * @param tablename The tablename to set.
   */
  public void setTablename(String tablename) {
    this.tablename = tablename;
  }

  /**
   * @param specifyFields The specify fields flag to set.
   */
  public void setSpecifyFields(boolean specifyFields) {
    this.specifyFields = specifyFields;
  }

  /**
   * @return Returns the specify fields flag.
   */
  public boolean specifyFields() {
    return specifyFields;
  }

  public boolean isDirect() {
    return direct;
  }

  public void setDirect(boolean direct) {
    this.direct = direct;
  }

  public boolean isAbortOnError() {
    return abortOnError;
  }

  public void setAbortOnError(boolean abortOnError) {
    this.abortOnError = abortOnError;
  }

  public String getExceptionsFileName() {
    return exceptionsFileName;
  }

  public void setExceptionsFileName(String exceptionsFileName) {
    this.exceptionsFileName = exceptionsFileName;
  }

  public String getRejectedDataFileName() {
    return rejectedDataFileName;
  }

  public void setRejectedDataFileName(String rejectedDataFileName) {
    this.rejectedDataFileName = rejectedDataFileName;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public boolean isSpecifyFields() {
    return specifyFields;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      this.databases = databases;
      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      schemaName = XmlHandler.getTagValue(transformNode, "schema");
      tablename = XmlHandler.getTagValue(transformNode, "table");

      // If not present it will be false to be compatible with pre-v3.2
      specifyFields = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "specify_fields"));

      Node fields = XmlHandler.getSubNode(transformNode, "fields"); // $NON-NLS-1$
      int nrRows = XmlHandler.countNodes(fields, "field"); // $NON-NLS-1$

      allocate(nrRows);

      for (int i = 0; i < nrRows; i++) {
        Node knode = XmlHandler.getSubNodeByNr(fields, "field", i); // $NON-NLS-1$

        fieldDatabase[i] = XmlHandler.getTagValue(knode, "column_name"); // $NON-NLS-1$
        fieldStream[i] = XmlHandler.getTagValue(knode, "stream_name"); // $NON-NLS-1$
      }

      exceptionsFileName = XmlHandler.getTagValue(transformNode, "exceptions_filename");
      rejectedDataFileName = XmlHandler.getTagValue(transformNode, "rejected_data_filename");
      abortOnError = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "abort_on_error"));
      direct = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "direct"));
      streamName = XmlHandler.getTagValue(transformNode, "stream_name");

    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public void setDefault() {
    databaseMeta = null;
    tablename = "";

    // To be compatible with pre-v3.2 (SB)
    specifyFields = false;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("    " + XmlHandler.addTagValue("schema", schemaName));
    retval.append("    " + XmlHandler.addTagValue("table", tablename));
    retval.append("    " + XmlHandler.addTagValue("specify_fields", specifyFields));

    retval.append("    <fields>").append(Const.CR); // $NON-NLS-1$

    for (int i = 0; i < fieldDatabase.length; i++) {
      retval.append("        <field>").append(Const.CR); // $NON-NLS-1$
      retval
          .append("          ")
          .append(
              XmlHandler.addTagValue("column_name", fieldDatabase[i])); // $NON-NLS-1$ //$NON-NLS-2$
      retval
          .append("          ")
          .append(
              XmlHandler.addTagValue("stream_name", fieldStream[i])); // $NON-NLS-1$ //$NON-NLS-2$
      retval.append("        </field>").append(Const.CR); // $NON-NLS-1$
    }
    retval.append("    </fields>").append(Const.CR); // $NON-NLS-1$

    retval.append("    " + XmlHandler.addTagValue("exceptions_filename", exceptionsFileName));
    retval.append("    " + XmlHandler.addTagValue("rejected_data_filename", rejectedDataFileName));
    retval.append("    " + XmlHandler.addTagValue("abort_on_error", abortOnError));
    retval.append("    " + XmlHandler.addTagValue("direct", direct));
    retval.append("    " + XmlHandler.addTagValue("stream_name", streamName));

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
    if (databaseMeta != null) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.ConnectionExists"),
              transformMeta);
      remarks.add(cr);

      Database db = new Database(loggingObject, variables, databaseMeta);

      try {
        db.connect();

        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.ConnectionOk"),
                transformMeta);
        remarks.add(cr);

        if (!StringUtil.isEmpty(tablename)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, db.resolve(schemaName), db.resolve(tablename));
          // Check if this table exists...
          String realSchemaName = db.resolve(schemaName);
          String realTableName = db.resolve(tablename);
          if (db.checkTableExists(realSchemaName, realTableName)) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "VerticaBulkLoaderMeta.CheckResult.TableAccessible", schemaTable),
                    transformMeta);
            remarks.add(cr);

            IRowMeta r = db.getTableFields(schemaTable);
            if (r != null) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "VerticaBulkLoaderMeta.CheckResult.TableOk", schemaTable),
                      transformMeta);
              remarks.add(cr);

              String error_message = "";
              boolean error_found = false;
              // OK, we have the table fields.
              // Now see what we can find as previous transform...
              if (prev != null && prev.size() > 0) {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_OK,
                        BaseMessages.getString(
                            PKG,
                            "VerticaBulkLoaderMeta.CheckResult.FieldsReceived",
                            "" + prev.size()),
                        transformMeta);
                remarks.add(cr);

                if (!specifyFields()) {
                  // Starting from prev...
                  for (int i = 0; i < prev.size(); i++) {
                    IValueMeta pv = prev.getValueMeta(i);
                    int idx = r.indexOfValue(pv.getName());
                    if (idx < 0) {
                      error_message +=
                          "\t\t" + pv.getName() + " (" + pv.getTypeDesc() + ")" + Const.CR;
                      error_found = true;
                    }
                  }
                  if (error_found) {
                    error_message =
                        BaseMessages.getString(
                            PKG,
                            "VerticaBulkLoaderMeta.CheckResult.FieldsNotFoundInOutput",
                            error_message);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_ERROR, error_message, transformMeta);
                    remarks.add(cr);
                  } else {
                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_OK,
                            BaseMessages.getString(
                                PKG, "VerticaBulkLoaderMeta.CheckResult.AllFieldsFoundInOutput"),
                            transformMeta);
                    remarks.add(cr);
                  }
                } else {
                  // Specifying the column names explicitly
                  for (int i = 0; i < getFieldDatabase().length; i++) {
                    int idx = r.indexOfValue(getFieldDatabase()[i]);
                    if (idx < 0) {
                      error_message += "\t\t" + getFieldDatabase()[i] + Const.CR;
                      error_found = true;
                    }
                  }
                  if (error_found) {
                    error_message =
                        BaseMessages.getString(
                            PKG,
                            "VerticaBulkLoaderMeta.CheckResult.FieldsSpecifiedNotInTable",
                            error_message);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_ERROR, error_message, transformMeta);
                    remarks.add(cr);
                  } else {
                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_OK,
                            BaseMessages.getString(
                                PKG, "VerticaBulkLoaderMeta.CheckResult.AllFieldsFoundInOutput"),
                            transformMeta);
                    remarks.add(cr);
                  }
                }

                error_message = "";
                if (!specifyFields()) {
                  // Starting from table fields in r...
                  for (int i = 0; i < getFieldDatabase().length; i++) {
                    IValueMeta rv = r.getValueMeta(i);
                    int idx = prev.indexOfValue(rv.getName());
                    if (idx < 0) {
                      error_message +=
                          "\t\t" + rv.getName() + " (" + rv.getTypeDesc() + ")" + Const.CR;
                      error_found = true;
                    }
                  }
                  if (error_found) {
                    error_message =
                        BaseMessages.getString(
                            PKG, "VerticaBulkLoaderMeta.CheckResult.FieldsNotFound", error_message);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_WARNING, error_message, transformMeta);
                    remarks.add(cr);
                  } else {
                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_OK,
                            BaseMessages.getString(
                                PKG, "VerticaBulkLoaderMeta.CheckResult.AllFieldsFound"),
                            transformMeta);
                    remarks.add(cr);
                  }
                } else {
                  // Specifying the column names explicitly
                  for (int i = 0; i < getFieldStream().length; i++) {
                    int idx = prev.indexOfValue(getFieldStream()[i]);
                    if (idx < 0) {
                      error_message += "\t\t" + getFieldStream()[i] + Const.CR;
                      error_found = true;
                    }
                  }
                  if (error_found) {
                    error_message =
                        BaseMessages.getString(
                            PKG,
                            "VerticaBulkLoaderMeta.CheckResult.FieldsSpecifiedNotFound",
                            error_message);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_ERROR, error_message, transformMeta);
                    remarks.add(cr);
                  } else {
                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_OK,
                            BaseMessages.getString(
                                PKG, "VerticaBulkLoaderMeta.CheckResult.AllFieldsFound"),
                            transformMeta);
                    remarks.add(cr);
                  }
                }
              } else {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_ERROR,
                        BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.NoFields"),
                        transformMeta);
                remarks.add(cr);
              }
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR,
                      BaseMessages.getString(
                          PKG, "VerticaBulkLoaderMeta.CheckResult.TableNotAccessible"),
                      transformMeta);
              remarks.add(cr);
            }
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG, "VerticaBulkLoaderMeta.CheckResult.TableError", schemaTable),
                    transformMeta);
            remarks.add(cr);
          }
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.NoTableName"),
                  transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "VerticaBulkLoaderMeta.CheckResult.UndefinedError", e.getMessage()),
                transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.NoConnection"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

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
    // The values that are entering this transform are in "prev":
    if (prev != null) {
      for (int i = 0; i < prev.size(); i++) {
        IValueMeta v = prev.getValueMeta(i);
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_WRITE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                tablename,
                v.getName(),
                v.getName(),
                v != null ? v.getOrigin() : "?",
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }
    }
  }

  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IHopMetadataProvider metadataProvider) {
    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && prev.size() > 0) {
        if (!StringUtil.isEmpty(tablename)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tablename);
            String cr_table = db.getDDL(schemaTable, prev);

            // Empty string means: nothing to do: set it to null...
            if (cr_table == null || cr_table.length() == 0) {
              cr_table = null;
            }

            retval.setSql(cr_table);
          } catch (HopDatabaseException dbe) {
            retval.setError(
                BaseMessages.getString(
                    PKG, "VerticaBulkLoaderMeta.Error.ErrorConnecting", dbe.getMessage()));
          } finally {
            db.disconnect();
          }
        } else {
          retval.setError(BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Error.NoTable"));
        }
      } else {
        retval.setError(BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Error.NoInput"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Error.NoConnection"));
    }

    return retval;
  }

  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tablename);
    String realSchemaName = variables.resolve(schemaName);

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!StringUtil.isEmpty(realTableName)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, realSchemaName, realTableName);

          // Check if this table exists...
          if (db.checkTableExists(realSchemaName, realTableName)) {
            return db.getTableFields(schemaTable);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Exception.ErrorGettingFields"), e);
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /**
   * @return Fields containing the values in the input stream to insert.
   */
  public String[] getFieldStream() {
    return fieldStream;
  }

  /**
   * @param fieldStream The fields containing the values in the input stream to insert in the table.
   */
  public void setFieldStream(String[] fieldStream) {
    this.fieldStream = fieldStream;
  }

  /**
   * @return Fields containing the fieldnames in the database insert.
   */
  public String[] getFieldDatabase() {
    return fieldDatabase;
  }

  /**
   * @param fieldDatabase The fields containing the names of the fields to insert.
   */
  public void setFieldDatabase(String[] fieldDatabase) {
    this.fieldDatabase = fieldDatabase;
  }

  /**
   * @return the schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName the schemaName to set
   */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /*    public IRowMeta getRequiredFields(IVariables variables) throws HopException {

      if ( databaseMeta != null ) {
          // TODO
          DbCache.getInstance().clear( databaseMeta.getName() );

          Database db = new Database(loggingObject, variables, databaseMeta );
          try {
              db.connect();

              if ( !StringUtil.isEmpty( tablename ) ) {
                  String schemaTable = databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tablename );

                  // Check if this table exists...
                  if ( db.checkTableExists( schemaTable ) ) {
                      return db.getTableFields( schemaTable );
                  } else {
                      throw new HopException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.TableNotFound" ) );
                  }
              } else {
                  throw new HopException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.TableNotSpecified" ) );
              }
          } catch ( Exception e ) {
              throw new HopException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.ErrorGettingFields" ),
                      e );
          } finally {
              db.disconnect();
          }
      } else {
          throw new HopException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.ConnectionNotDefined" ) );
      }

  }*/

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    // use default message
    return null;
  }

  @Override
  public RowMeta getRowMeta(IVariables variables, ITransformData transformData) {
    return (RowMeta) ((VerticaBulkLoaderData) transformData).getInsertRowMeta();
  }

  @Override
  public List<String> getDatabaseFields() {
    if (specifyFields()) {
      return Arrays.asList(getFieldDatabase());
    }
    return Collections.emptyList();
  }

  @Override
  public List<String> getStreamFields() {
    if (specifyFields()) {
      return Arrays.asList(getFieldStream());
    }
    return Collections.emptyList();
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = (fieldDatabase == null) ? -1 : fieldDatabase.length;
    if (nrFields <= 0) {
      return;
    }
    String[][] rtn = Utils.normalizeArrays(nrFields, fieldStream);
    fieldStream = rtn[0];
  }
}
