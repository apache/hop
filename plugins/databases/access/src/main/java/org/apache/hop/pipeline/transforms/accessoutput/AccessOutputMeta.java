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
package org.apache.hop.pipeline.transforms.accessoutput;

import io.github.spannm.jackcess.Column;
import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.Table;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "AccessOutput",
    name = "i18n::AccessOutput.Name",
    description = "i18n::AccessOutput.Description",
    image = "accessoutput.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::AccessOutput.Keyword",
    documentationUrl = "/pipeline/transforms/accessoutput.html",
    classLoaderGroup = "access-db",
    isIncludeJdbcDrivers = true)
public class AccessOutputMeta extends BaseTransformMeta<AccessOutput, AccessOutputData> {
  private static final Class<?> PKG = AccessOutputMeta.class;

  public static final int COMMIT_SIZE = 500;

  @HopMetadataProperty(
      key = "filename",
      injectionKeyDescription = "AccessOutputMeta.Injection.FILE_NAME")
  private String fileName;

  @HopMetadataProperty(
      key = "fileformat",
      storeWithCode = true,
      injectionKeyDescription = "AccessOutputMeta.Injection.FILE_FORMAT")
  private AccessFileFormat fileFormat;

  @HopMetadataProperty(
      key = "tablename",
      injectionKeyDescription = "AccessOutputMeta.Injection.TABLE_NAME")
  private String tableName;

  @HopMetadataProperty(
      key = "create_table",
      injectionKeyDescription = "AccessOutputMeta.Injection.CREATE_TABLE")
  private boolean createTable;

  @HopMetadataProperty(
      key = "create_file",
      injectionKeyDescription = "AccessOutputMeta.Injection.CREATE_FILE")
  private boolean createFile;

  @HopMetadataProperty(
      key = "truncate",
      injectionKeyDescription = "AccessOutputMeta.Injection.TRUNCATE")
  private boolean truncateTable;

  @HopMetadataProperty(
      key = "commit_size",
      injectionKeyDescription = "AccessOutputMeta.Injection.COMMIT_SIZE")
  private int commitSize;

  /** Flag: add the filename to result filenames */
  @HopMetadataProperty(
      key = "add_to_result_filenames",
      injectionKeyDescription = "AccessOutputMeta.Injection.ADD_TO_RESULT_FILE")
  private boolean addToResultFile;

  /** Flag : Do not create new file when transformation start, wait the first row */
  @HopMetadataProperty(
      key = "do_not_open_newfile_init",
      injectionKeyDescription = "AccessOutputMeta.Injection.WAIT_FIRST_ROW_TO_CREATE_FILE")
  private boolean waitFirstRowToCreateFile;

  public AccessOutputMeta() {
    super();
  }

  @Override
  public Object clone() {
    AccessOutputMeta retval = (AccessOutputMeta) super.clone();
    return retval;
  }

  /**
   * @return Returns the tablename.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tablename The tablename to set.
   */
  public void setTableName(String tablename) {
    this.tableName = tablename;
  }

  /**
   * @param truncateTable The truncate table flag to set.
   */
  public void setTruncateTable(boolean truncateTable) {
    this.truncateTable = truncateTable;
  }

  @Override
  public void setDefault() {
    createFile = true;
    fileFormat = AccessFileFormat.V2019;
    createTable = true;
    truncateTable = false;
    commitSize = COMMIT_SIZE;
    waitFirstRowToCreateFile = false;
    addToResultFile = true;
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

    // See if we have input streams leading to this transformation
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AccessOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AccessOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    if (Utils.isEmpty(fileName)) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AccessOutputMeta.CheckResult.MissingDatabaseFileName"),
              transformMeta);
      remarks.add(cr);
    }

    if (Utils.isEmpty(tableName)) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AccessOutputMeta.CheckResult.MissingTableName"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realFilename = variables.resolve(fileName);
    File file = new File(realFilename);
    Database db = null;
    try {
      if (!file.exists() || !file.isFile()) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "AccessOutputMeta.Exception.FileDoesNotExist", realFilename));
      }

      // open the database and get the table
      db = DatabaseBuilder.open(file);
      String realTablename = variables.resolve(tableName);
      Table table = db.getTable(realTablename);
      if (table == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "AccessOutputMeta.Exception.TableDoesNotExist", realTablename));
      }

      return getLayout(table);
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "AccessOutputMeta.Exception.ErrorGettingFields"), e);
    } finally {
      try {
        if (db != null) {
          db.close();
        }
      } catch (IOException e) {
        throw new HopException(
            BaseMessages.getString(PKG, "AccessOutputMeta.Exception.ErrorClosingDatabase"), e);
      }
    }
  }

  public static final IRowMeta getLayout(Table table) throws HopTransformException, IOException {
    IRowMeta row = new RowMeta();
    for (Column column : table.getColumns()) {

      int valtype = IValueMeta.TYPE_STRING;
      int length = -1;
      int precision = -1;

      int type = column.getType().getSQLType();
      switch (type) {
        case java.sql.Types.CHAR,
            java.sql.Types.VARCHAR,
            java.sql.Types.LONGVARCHAR: // Character Large Object
          valtype = IValueMeta.TYPE_STRING;
          length = column.getLength();
          break;

        case java.sql.Types.CLOB:
          valtype = IValueMeta.TYPE_STRING;
          length = DatabaseMeta.CLOB_LENGTH;
          break;

        case java.sql.Types.BIGINT:
          valtype = IValueMeta.TYPE_INTEGER;
          precision = 0; // Max 9.223.372.036.854.775.807
          length = 15;
          break;

        case java.sql.Types.INTEGER:
          valtype = IValueMeta.TYPE_INTEGER;
          precision = 0; // Max 2.147.483.647
          length = 9;
          break;

        case java.sql.Types.SMALLINT:
          valtype = IValueMeta.TYPE_INTEGER;
          precision = 0; // Max 32.767
          length = 4;
          break;

        case java.sql.Types.TINYINT:
          valtype = IValueMeta.TYPE_INTEGER;
          precision = 0; // Max 127
          length = 2;
          break;

        case java.sql.Types.DECIMAL,
            java.sql.Types.DOUBLE,
            java.sql.Types.FLOAT,
            java.sql.Types.REAL,
            java.sql.Types.NUMERIC:
          valtype = IValueMeta.TYPE_NUMBER;
          length = column.getLength();
          precision = column.getPrecision();
          if (length >= 126) {
            length = -1;
          }
          if (precision >= 126) {
            precision = -1;
          }

          if (type == java.sql.Types.DOUBLE
              || type == java.sql.Types.FLOAT
              || type == java.sql.Types.REAL) {
            if (precision == 0) {
              precision = -1; // precision is obviously incorrect if the type if Double/Float/Real
            }
          } else {
            if (precision == 0
                && length < 18
                && length > 0) { // Among others Oracle is affected here.
              valtype = IValueMeta.TYPE_INTEGER;
            }
          }
          if (length > 18 || precision > 18) {
            valtype = IValueMeta.TYPE_BIGNUMBER;
          }

          break;

        case java.sql.Types.DATE, java.sql.Types.TIME, java.sql.Types.TIMESTAMP:
          valtype = IValueMeta.TYPE_DATE;
          break;

        case java.sql.Types.BOOLEAN, java.sql.Types.BIT:
          valtype = IValueMeta.TYPE_BOOLEAN;
          break;

        case java.sql.Types.BINARY,
            java.sql.Types.BLOB,
            java.sql.Types.VARBINARY,
            java.sql.Types.LONGVARBINARY:
          valtype = IValueMeta.TYPE_BINARY;
          break;

        default:
          valtype = IValueMeta.TYPE_STRING;
          length = column.getLength();
          break;
      }

      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta(column.getName(), valtype);
      } catch (HopPluginException e) {
        throw new HopTransformException(e);
      }
      v.setLength(length, precision);
      row.addValueMeta(v);
    }

    return row;
  }

  /**
   * @return the fileCreated
   */
  public boolean isCreateFile() {
    return createFile;
  }

  /**
   * @param fileCreated the fileCreated to set
   */
  public void setCreateFile(boolean fileCreated) {
    this.createFile = fileCreated;
  }

  /**
   * @return the filename
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @param filename the filename to set
   */
  public void setFileName(String filename) {
    this.fileName = filename;
  }

  public AccessFileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(AccessFileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  /**
   * @return the tableCreated
   */
  public boolean isCreateTable() {
    return createTable;
  }

  /**
   * @param tableCreated the tableCreated to set
   */
  public void setCreateTable(boolean tableCreated) {
    this.createTable = tableCreated;
  }

  /**
   * @return the tableTruncated
   */
  public boolean isTruncateTable() {
    return truncateTable;
  }

  /**
   * @return the commitSize
   */
  public int getCommitSize() {
    return commitSize;
  }

  /**
   * @param commitSize the commitSize to set
   */
  public void setCommitSize(int commitSize) {
    this.commitSize = commitSize;
  }

  /**
   * @return Returns the add to result filesname.
   */
  public boolean isAddToResultFile() {
    return addToResultFile;
  }

  /**
   * @param addtoresultfilenamesin The addtoresultfilenames to set.
   */
  public void setAddToResultFile(boolean addtoresultfilenamesin) {
    this.addToResultFile = addtoresultfilenamesin;
  }

  /**
   * @return Returns the "do not open new file init" flag
   */
  public boolean isWaitFirstRowToCreateFile() {
    return waitFirstRowToCreateFile;
  }

  /**
   * @param doNotOpenNewFileInit The "do not open new file init" flag to set.
   */
  public void setWaitFirstRowToCreateFile(boolean doNotOpenNewFileInit) {
    this.waitFirstRowToCreateFile = doNotOpenNewFileInit;
  }

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    // The object that we're modifying here is a copy of the original!
    // So let's change the filename from relative to absolute by grabbing the file object...
    //
    try {
      if (!Utils.isEmpty(fileName)) {
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileName));
        fileName = iResourceNaming.nameResource(fileObject, variables, true);
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
