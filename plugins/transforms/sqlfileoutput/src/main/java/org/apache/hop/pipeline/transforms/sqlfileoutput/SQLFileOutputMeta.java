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
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Transform(
    id = "SQLFileOutput",
    image = "sqlfileoutput.svg",
    name = "i18n::SQLFileOutput.Name",
    description = "i18n::SQLFileOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/sqlfileoutput.html")
public class SQLFileOutputMeta extends BaseTransformMeta
    implements ITransformMeta<SQLFileOutput, SQLFileOutputData> {
  private static final Class<?> PKG = SQLFileOutputMeta.class; // For Translator

  private DatabaseMeta databaseMeta;
  private String schemaName;
  private String tableName;
  private boolean truncateTable;

  private boolean AddToResult;

  private boolean createTable;

  /** The base name of the output file */
  private String fileName;

  /** The file extention in case of a generated filename */
  private String extension;

  /**
   * if this value is larger then 0, the text file is split up into parts of this number of lines
   */
  private int splitEvery;

  /** Flag to indicate the we want to append to the end of an existing file (if it exists) */
  private boolean fileAppended;

  /** Flag: add the transformnr in the filename */
  private boolean transformNrInFilename;

  /** Flag: add the partition number in the filename */
  private boolean partNrInFilename;

  /** Flag: add the date in the filename */
  private boolean dateInFilename;

  /** Flag: add the time in the filename */
  private boolean timeInFilename;

  /** The encoding to use for reading: null or empty string means system default encoding */
  private String encoding;

  /** The date format */
  private String dateformat;

  /** Start New line for each statement */
  private boolean StartNewLine;

  /** Flag: create parent folder if needed */
  private boolean createparentfolder;

  private boolean DoNotOpenNewFileInit;

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {

    SQLFileOutputMeta retval = (SQLFileOutputMeta) super.clone();

    return retval;
  }

  /** @return Returns the database. */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param database The database to set. */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /** @return Returns the extension. */
  public String getExtension() {
    return extension;
  }

  /** @param extension The extension to set. */
  public void setExtension(String extension) {
    this.extension = extension;
  }

  /** @return Returns the fileAppended. */
  public boolean isFileAppended() {
    return fileAppended;
  }

  /** @param fileAppended The fileAppended to set. */
  public void setFileAppended(boolean fileAppended) {
    this.fileAppended = fileAppended;
  }

  /** @return Returns the fileName. */
  public String getFileName() {
    return fileName;
  }

  /** @return Returns the splitEvery. */
  public int getSplitEvery() {
    return splitEvery;
  }

  /** @param splitEvery The splitEvery to set. */
  public void setSplitEvery(int splitEvery) {
    this.splitEvery = splitEvery;
  }

  /** @return Returns the transformNrInFilename. */
  public boolean isTransformNrInFilename() {
    return transformNrInFilename;
  }

  /** @param transformNrInFilename The transformNrInFilename to set. */
  public void setTransformNrInFilename(boolean transformNrInFilename) {
    this.transformNrInFilename = transformNrInFilename;
  }

  /** @return Returns the timeInFilename. */
  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  /** @return Returns the dateInFilename. */
  public boolean isDateInFilename() {
    return dateInFilename;
  }

  /** @param dateInFilename The dateInFilename to set. */
  public void setDateInFilename(boolean dateInFilename) {
    this.dateInFilename = dateInFilename;
  }

  /** @param timeInFilename The timeInFilename to set. */
  public void setTimeInFilename(boolean timeInFilename) {
    this.timeInFilename = timeInFilename;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /**
   * @return The desired encoding of output file, null or empty if the default system encoding needs
   *     to be used.
   */
  public String getEncoding() {
    return encoding;
  }

  /** @return The desired date format. */
  public String getDateFormat() {
    return dateformat;
  }

  /**
   * @param encoding The desired encoding of output file, null or empty if the default system
   *     encoding needs to be used.
   */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /** @param dateFormat The desired date format of output field date used. */
  public void setDateFormat(String dateFormat) {
    this.dateformat = dateFormat;
  }

  /** @return Returns the table name. */
  public String getTablename() {
    return tableName;
  }

  /** @param tableName The table name to set. */
  public void setTablename(String tableName) {
    this.tableName = tableName;
  }

  /** @return Returns the truncate table flag. */
  public boolean truncateTable() {
    return truncateTable;
  }

  /** @return Returns the Add to result filesname flag. */
  public boolean AddToResult() {
    return AddToResult;
  }

  /** @return Returns the Start new line flag. */
  public boolean StartNewLine() {
    return StartNewLine;
  }

  public boolean isDoNotOpenNewFileInit() {
    return DoNotOpenNewFileInit;
  }

  public void setDoNotOpenNewFileInit(boolean DoNotOpenNewFileInit) {
    this.DoNotOpenNewFileInit = DoNotOpenNewFileInit;
  }

  /** @return Returns the create table flag. */
  public boolean createTable() {
    return createTable;
  }

  /** @param truncateTable The truncate table flag to set. */
  public void setTruncateTable(boolean truncateTable) {
    this.truncateTable = truncateTable;
  }

  /** @param AddToResult The Add file to result to set. */
  public void setAddToResult(boolean AddToResult) {
    this.AddToResult = AddToResult;
  }

  /** @param StartNewLine The Start NEw Line to set. */
  public void setStartNewLine(boolean StartNewLine) {
    this.StartNewLine = StartNewLine;
  }

  /** @param createTable The create table flag to set. */
  public void setCreateTable(boolean createTable) {
    this.createTable = createTable;
  }

  /** @return Returns the create parent folder flag. */
  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /** @param createparentfolder The create parent folder flag to set. */
  public void setCreateParentFolder(boolean createparentfolder) {
    this.createparentfolder = createparentfolder;
  }

  public String[] getFiles(IVariables variables, String fileName) {
    int copies = 1;
    int splits = 1;
    int parts = 1;

    if (transformNrInFilename) {
      copies = 3;
    }

    if (partNrInFilename) {
      parts = 3;
    }

    if (splitEvery != 0) {
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

    if (dateInFilename) {
      daf.applyPattern("yyyMMdd");
      String d = daf.format(now);
      retval += "_" + d;
    }
    if (timeInFilename) {
      daf.applyPattern("HHmmss");
      String t = daf.format(now);
      retval += "_" + t;
    }
    if (transformNrInFilename) {
      retval += "_" + transformnr;
    }

    if (splitEvery > 0) {
      retval += "_" + splitnr;
    }

    if (extension != null && extension.length() != 0) {
      retval += "." + variables.resolve(extension);
    }

    return retval;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {

      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      schemaName = XmlHandler.getTagValue(transformNode, "schema");
      tableName = XmlHandler.getTagValue(transformNode, "table");
      truncateTable = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "truncate"));
      createTable = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "create"));
      encoding = XmlHandler.getTagValue(transformNode, "encoding");
      dateformat = XmlHandler.getTagValue(transformNode, "dateformat");
      AddToResult = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "AddToResult"));

      StartNewLine = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "StartNewLine"));

      fileName = XmlHandler.getTagValue(transformNode, "file", "name");
      createparentfolder =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "file", "create_parent_folder"));
      extension = XmlHandler.getTagValue(transformNode, "file", "extention");
      fileAppended = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "append"));
      transformNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "split"));
      partNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "haspartno"));
      dateInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_date"));
      timeInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_time"));
      splitEvery = Const.toInt(XmlHandler.getTagValue(transformNode, "file", "splitevery"), 0);
      DoNotOpenNewFileInit =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "file", "DoNotOpenNewFileInit"));

    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public void setDefault() {
    databaseMeta = null;
    tableName = "";
    createparentfolder = false;
    DoNotOpenNewFileInit = false;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("    " + XmlHandler.addTagValue("schema", schemaName));
    retval.append("    " + XmlHandler.addTagValue("table", tableName));
    retval.append("    " + XmlHandler.addTagValue("truncate", truncateTable));
    retval.append("    " + XmlHandler.addTagValue("create", createTable));
    retval.append("    " + XmlHandler.addTagValue("encoding", encoding));
    retval.append("    " + XmlHandler.addTagValue("dateformat", dateformat));
    retval.append("    " + XmlHandler.addTagValue("addtoresult", AddToResult));

    retval.append("    " + XmlHandler.addTagValue("startnewline", StartNewLine));

    retval.append("    <file>" + Const.CR);
    retval.append("      " + XmlHandler.addTagValue("name", fileName));
    retval.append("      " + XmlHandler.addTagValue("extention", extension));
    retval.append("      " + XmlHandler.addTagValue("append", fileAppended));
    retval.append("      " + XmlHandler.addTagValue("split", transformNrInFilename));
    retval.append("      " + XmlHandler.addTagValue("haspartno", partNrInFilename));
    retval.append("      " + XmlHandler.addTagValue("add_date", dateInFilename));
    retval.append("      " + XmlHandler.addTagValue("add_time", timeInFilename));
    retval.append("      " + XmlHandler.addTagValue("splitevery", splitEvery));
    retval.append("      " + XmlHandler.addTagValue("create_parent_folder", createparentfolder));
    retval.append("      " + XmlHandler.addTagValue("DoNotOpenNewFileInit", DoNotOpenNewFileInit));

    retval.append("      </file>" + Const.CR);

    return retval.toString();
  }

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
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.ConnectionExists"),
              transformMeta);
      remarks.add(cr);

      Database db = new Database(loggingObject, variables, databaseMeta );
      try {
        db.connect();

        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
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
                    CheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "SQLFileOutputMeta.CheckResult.TableAccessible", schemaTable),
                    transformMeta);
            remarks.add(cr);

            IRowMeta r = db.getTableFieldsMeta(schemaName, tableName);
            if (r != null) {
              cr =
                  new CheckResult(
                      CheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "SQLFileOutputMeta.CheckResult.TableOk", schemaTable),
                      transformMeta);
              remarks.add(cr);

              String errorMessage = "";
              boolean errorFound = false;
              // OK, we have the table fields.
              // Now see what we can find as previous transform...
              if (prev != null && prev.size() > 0) {
                cr =
                    new CheckResult(
                        CheckResult.TYPE_RESULT_OK,
                        BaseMessages.getString(
                            PKG, "SQLFileOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
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

                  cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
                  remarks.add(cr);
                } else {
                  cr =
                      new CheckResult(
                          CheckResult.TYPE_RESULT_OK,
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
                      new CheckResult(CheckResult.TYPE_RESULT_WARNING, errorMessage, transformMeta);
                  remarks.add(cr);
                } else {
                  cr =
                      new CheckResult(
                          CheckResult.TYPE_RESULT_OK,
                          BaseMessages.getString(
                              PKG, "SQLFileOutputMeta.CheckResult.AllFieldsFound"),
                          transformMeta);
                  remarks.add(cr);
                }
              } else {
                cr =
                    new CheckResult(
                        CheckResult.TYPE_RESULT_ERROR,
                        BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.NoFields"),
                        transformMeta);
                remarks.add(cr);
              }
            } else {
              cr =
                  new CheckResult(
                      CheckResult.TYPE_RESULT_ERROR,
                      BaseMessages.getString(
                          PKG, "SQLFileOutputMeta.CheckResult.TableNotAccessible"),
                      transformMeta);
              remarks.add(cr);
            }
          } else {
            cr =
                new CheckResult(
                    CheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG, "SQLFileOutputMeta.CheckResult.TableError", schemaTable),
                    transformMeta);
            remarks.add(cr);
          }
        } else {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.NoTableName"),
                  transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
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
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.NoConnection"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SQLFileOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public ITransform createTransform(
      TransformMeta transformMeta,
      SQLFileOutputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SQLFileOutput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public SQLFileOutputData getTransformData() {
    return new SQLFileOutputData();
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
        if (!Utils.isEmpty(tableName)) {
          Database db = new Database(loggingObject, variables, databaseMeta );
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
            String crTable = db.getDDL(schemaTable, prev);

            // Empty string means: nothing to do: set it to null...
            if (crTable == null || crTable.length() == 0) {
              crTable = null;
            }

            retval.setSql(crTable);
          } catch (HopDatabaseException dbe) {
            retval.setError(
                BaseMessages.getString(
                    PKG, "SQLFileOutputMeta.Error.ErrorConnecting", dbe.getMessage()));
          } finally {
            db.disconnect();
          }
        } else {
          retval.setError(
              BaseMessages.getString(PKG, "SQLFileOutputMeta.Exception.TableNotSpecified"));
        }
      } else {
        retval.setError(BaseMessages.getString(PKG, "SQLFileOutputMeta.Error.NoInput"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "SQLFileOutputMeta.Error.NoConnection"));
    }

    return retval;
  }

  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta );
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

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /** @return the schemaName */
  public String getSchemaName() {
    return schemaName;
  }

  /** @param schemaName the schemaName to set */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

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
      FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileName));

      // If the file doesn't exist, forget about this effort too!
      //
      if (fileObject.exists()) {
        // Convert to an absolute path...
        //
        fileName = iResourceNaming.nameResource(fileObject, variables, true);

        return fileName;
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
