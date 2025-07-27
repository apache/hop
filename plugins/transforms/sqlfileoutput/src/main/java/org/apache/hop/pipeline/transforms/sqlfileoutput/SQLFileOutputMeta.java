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

package org.apache.hop.pipeline.transforms.sqlfileoutput;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

@Transform(
    id = "SQLFileOutput",
    image = "sqlfileoutput.svg",
    name = "i18n::SQLFileOutput.Name",
    description = "i18n::SQLFileOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::SQLFileOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/sqlfileoutput.html")
@Getter
@Setter
public class SQLFileOutputMeta extends BaseTransformMeta<SQLFileOutput, SQLFileOutputData> {
  private static final Class<?> PKG = SQLFileOutputMeta.class;
  private static final String CONST_SPACE = "      ";
  private static final String CONST_SPACE_SHORT = "    ";

  @HopMetadataProperty(key = "connection")
  private String connection;

  @HopMetadataProperty(key = "schema")
  private String schemaName;

  @HopMetadataProperty(key = "table")
  private String tableName;

  @HopMetadataProperty(key = "truncate")
  private boolean truncateTable;

  @HopMetadataProperty(key = "AddToResult")
  private boolean addToResult;

  @HopMetadataProperty(key = "create")
  private boolean createTable;

  @HopMetadataProperty(key = "file")
  private SqlFile file;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(key = "encoding")
  private String encoding;

  /** The date format */
  @HopMetadataProperty(key = "dateformat")
  private String dateFormat;

  /** Start New line for each statement */
  @HopMetadataProperty(key = "StartNewLine")
  private boolean startNewLine;

  /**
   * @deprecated keep for backwards compatibility
   * @param transformNode the XML of the transform node
   * @param metadataProvider the metadata provider
   * @throws HopXmlException when unable to parse the XML
   */
  @Override
  @Deprecated(since = "2.13")
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      super.loadXml(transformNode, metadataProvider);

      // Rename from extention to extension
      file.extension = XmlHandler.getTagValue(transformNode, "file", "extention");
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public String[] getFiles(IVariables variables, String fileName) {
    int copies = 1;
    int splits = 1;
    int parts = 1;

    if (file.transformNrInFilename) {
      copies = 3;
    }

    if (file.partNrInFilename) {
      parts = 3;
    }

    if (file.splitEvery != 0) {
      splits = 3;
    }

    int nr = copies * parts * splits;
    if (nr > 1) {
      nr++;
    }

    String[] retval = new String[nr];

    int i = 0;
    for (int copy = 0; copy < copies; copy++) {
      for (int part = 0; part < parts; part++) {
        for (int split = 0; split < splits; split++) {
          retval[i] = buildFilename(variables, fileName, copy, split);
          i++;
        }
      }
    }
    if (i < nr) {
      retval[i] = "...";
    }

    return retval;
  }

  public String buildFilename(IVariables variables, String fileName, int transformnr, int splitnr) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = fileName;

    Date now = new Date();

    if (file.dateInFilename) {
      daf.applyPattern("yyyMMdd");
      String d = daf.format(now);
      retval += "_" + d;
    }
    if (file.timeInFilename) {
      daf.applyPattern("HHmmss");
      String t = daf.format(now);
      retval += "_" + t;
    }
    if (file.transformNrInFilename) {
      retval += "_" + transformnr;
    }

    if (file.splitEvery > 0) {
      retval += "_" + splitnr;
    }

    if (file.extension != null && !file.extension.isEmpty()) {
      retval += "." + variables.resolve(file.extension);
    }

    return retval;
  }

  @Override
  public void setDefault() {
    file = new SqlFile();
    connection = "";
    tableName = "";
    file.createParentFolder = false;
    file.doNotOpenNewFileInit = false;
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

    Database db = null;

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      if (databaseMeta != null) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.ConnectionExists"),
                transformMeta);
        remarks.add(cr);

        db = new Database(loggingObject, variables, databaseMeta);
        try {
          db.connect();

          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.ConnectionOk"),
                  transformMeta);
          remarks.add(cr);

          if (!Utils.isEmpty(tableName)) {
            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
            // Check if this table exists...
            if (db.checkTableExists(schemaName, tableName)) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "SQLFileOutputMeta.CheckResult.TableAccessible", schemaTable),
                      transformMeta);
              remarks.add(cr);

              IRowMeta r = db.getTableFieldsMeta(schemaName, tableName);
              if (r != null) {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_OK,
                        BaseMessages.getString(
                            PKG, "SQLFileOutputMeta.CheckResult.TableOk", schemaTable),
                        transformMeta);
                remarks.add(cr);

                String errorMessage = "";
                boolean errorFound = false;
                // OK, we have the table fields.
                // Now see what we can find as previous transform...
                if (prev != null && !prev.isEmpty()) {
                  cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_OK,
                          BaseMessages.getString(
                              PKG,
                              "SQLFileOutputMeta.CheckResult.FieldsReceived",
                              "" + prev.size()),
                          transformMeta);
                  remarks.add(cr);

                  // Starting from prev...
                  for (int i = 0; i < prev.size(); i++) {
                    IValueMeta pv = prev.getValueMeta(i);
                    int idx = r.indexOfValue(pv.getName());
                    if (idx < 0) {
                      errorMessage +=
                          "\t\t" + pv.getName() + " (" + pv.getTypeDesc() + ")" + Const.CR;
                      errorFound = true;
                    }
                  }
                  if (errorFound) {
                    errorMessage =
                        BaseMessages.getString(
                            PKG,
                            "SQLFileOutputMeta.CheckResult.FieldsNotFoundInOutput",
                            errorMessage);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
                    remarks.add(cr);
                  } else {
                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_OK,
                            BaseMessages.getString(
                                PKG, "SQLFileOutputMeta.CheckResult.AllFieldsFoundInOutput"),
                            transformMeta);
                    remarks.add(cr);
                  }

                  // Starting from table fields in r...
                  for (int i = 0; i < r.size(); i++) {
                    IValueMeta rv = r.getValueMeta(i);
                    int idx = prev.indexOfValue(rv.getName());
                    if (idx < 0) {
                      errorMessage +=
                          "\t\t" + rv.getName() + " (" + rv.getTypeDesc() + ")" + Const.CR;
                      errorFound = true;
                    }
                  }
                  if (errorFound) {
                    errorMessage =
                        BaseMessages.getString(
                            PKG, "SQLFileOutputMeta.CheckResult.FieldsNotFound", errorMessage);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_WARNING, errorMessage, transformMeta);
                    remarks.add(cr);
                  } else {
                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_OK,
                            BaseMessages.getString(
                                PKG, "SQLFileOutputMeta.CheckResult.AllFieldsFound"),
                            transformMeta);
                    remarks.add(cr);
                  }
                } else {
                  cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_ERROR,
                          BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.NoFields"),
                          transformMeta);
                  remarks.add(cr);
                }
              } else {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_ERROR,
                        BaseMessages.getString(
                            PKG, "SQLFileOutputMeta.CheckResult.TableNotAccessible"),
                        transformMeta);
                remarks.add(cr);
              }
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR,
                      BaseMessages.getString(
                          PKG, "SQLFileOutputMeta.CheckResult.TableError", schemaTable),
                      transformMeta);
              remarks.add(cr);
            }
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.NoTableName"),
                    transformMeta);
            remarks.add(cr);
          }
        } catch (HopException e) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "SQLFileOutputMeta.CheckResult.UndefinedError", e.getMessage()),
                  transformMeta);
          remarks.add(cr);
        } finally {
          db.disconnect();
        }
      } else {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.NoConnection"),
                transformMeta);
        remarks.add(cr);
      }
    } catch (HopException e) {
      String errorMessage =
          BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.DatabaseErrorOccurred")
              + e.getMessage();
      CheckResult cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } finally {
      db.disconnect();
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
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
    DatabaseMeta databaseMeta = null;
    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
      if (truncateTable) {
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_TRUNCATE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                tableName,
                "",
                "",
                "",
                "",
                "Truncate of table");
        impact.add(ii);
      }
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
                  tableName,
                  v.getName(),
                  v.getName(),
                  v != null ? v.getOrigin() : "?",
                  "",
                  "Type = " + v.toStringMeta());
          impact.add(ii);
        }
      }
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection));
    }
  }

  @Override
  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IHopMetadataProvider metadataProvider) {

    SqlStatement retVal = null;
    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
      retVal =
          new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

      if (databaseMeta != null) {
        if (prev != null && !prev.isEmpty()) {
          if (!Utils.isEmpty(tableName)) {
            Database db = new Database(loggingObject, variables, databaseMeta);
            try {
              db.connect();

              String schemaTable =
                  databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
              String crTable = db.getDDL(schemaTable, prev);

              // Empty string means: nothing to do: set it to null...
              if (crTable == null || crTable.isEmpty()) {
                crTable = null;
              }

              retVal.setSql(crTable);
            } catch (HopDatabaseException dbe) {
              retVal.setError(
                  BaseMessages.getString(
                      PKG, "SQLFileOutputMeta.Error.ErrorConnecting", dbe.getMessage()));
            } finally {
              db.disconnect();
            }
          } else {
            retVal.setError(
                BaseMessages.getString(PKG, "SQLFileOutputMeta.Exception.TableNotSpecified"));
          }
        } else {
          retVal.setError(BaseMessages.getString(PKG, "SQLFileOutputMeta.Error.NoInput"));
        }
      } else {
        retVal.setError(BaseMessages.getString(PKG, "SQLFileOutputMeta.Error.NoConnection"));
      }
    } catch (HopException e) {
      retVal.setError(
          BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.DatabaseErrorOccurred"));
    }
    return retVal;
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!Utils.isEmpty(realTableName)) {
          // Check if this table exists...
          if (db.checkTableExists(realSchemaName, realTableName)) {
            return db.getTableFieldsMeta(realSchemaName, realTableName);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "SQLFileOutputMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "SQLFileOutputMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "SQLFileOutputMeta.Exception.ErrorGettingFields"), e);
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "SQLFileOutputMeta.Exception.ConnectionNotDefined"));
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files
   * relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
   *
   * @param variables the variable variables to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      //
      // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.data
      // To : /home/matt/test/files/foo/bar.data
      //
      FileObject fileObject = HopVfs.getFileObject(variables.resolve(file.fileName), variables);

      // If the file doesn't exist, forget about this effort too!
      //
      if (fileObject.exists()) {
        // Convert to an absolute path...
        //
        file.fileName = iResourceNaming.nameResource(fileObject, variables, true);

        return file.fileName;
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Getter
  @Setter
  public static final class SqlFile {
    @HopMetadataProperty(key = "name")
    private String fileName;

    @HopMetadataProperty(key = "create_parent_folder")
    private boolean createParentFolder;

    @HopMetadataProperty(key = "DoNotOpenNewFileInit")
    private boolean doNotOpenNewFileInit;

    @HopMetadataProperty(key = "extension")
    private String extension;

    /**
     * if this value is larger then 0, the text file is split up into parts of this number of lines
     */
    @HopMetadataProperty(key = "splitevery")
    private int splitEvery;

    /** Flag to indicate the we want to append to the end of an existing file (if it exists) */
    @HopMetadataProperty(key = "append")
    private boolean fileAppended;

    /** Flag: add the transformnr in the filename */
    @HopMetadataProperty(key = "split")
    private boolean transformNrInFilename;

    /** Flag: add the partition number in the filename */
    @HopMetadataProperty(key = "haspartno")
    private boolean partNrInFilename;

    /** Flag: add the date in the filename */
    @HopMetadataProperty(key = "add_date")
    private boolean dateInFilename;

    /** Flag: add the time in the filename */
    @HopMetadataProperty(key = "add_time")
    private boolean timeInFilename;
  }
}
