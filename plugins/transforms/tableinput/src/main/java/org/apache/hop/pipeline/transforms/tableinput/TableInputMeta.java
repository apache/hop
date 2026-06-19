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

package org.apache.hop.pipeline.transforms.tableinput;

import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Transform(
    id = "TableInput",
    image = "tableinput.svg",
    name = "i18n::TableInput.Name",
    description = "i18n::TableInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/tableinput.html",
    keywords = "i18n::TableInputMeta.keyword",
    actionTransformTypes = {ActionTransformType.INPUT, ActionTransformType.RDBMS})
@Getter
@Setter
public class TableInputMeta extends BaseTransformMeta<TableInput, TableInputData> {

  private static final Class<?> PKG = TableInputMeta.class;

  @HopMetadataProperty(
      key = "sql",
      injectionKey = "SQL",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SQL_SELECT)
  private String sql;

  @HopMetadataProperty(key = "limit", injectionKey = "LIMIT")
  private String rowLimit;

  /** Should I execute once per row? */
  @HopMetadataProperty(key = "execute_each_row", injectionKey = "EXECUTE_FOR_EACH_ROW")
  private boolean executeEachInputRow;

  @HopMetadataProperty(key = "variables_active", injectionKey = "REPLACE_VARIABLES")
  private boolean variableReplacementActive;

  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTIONNAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty private String lookup;

  /**
   * When set, SQL is loaded from this file (VFS path, supports variables). SQL editor is read-only.
   */
  @HopMetadataProperty(key = "sql_from_file", injectionKey = "SQL_FROM_FILE")
  private String sqlFromFile;

  public TableInputMeta() {
    super();
  }

  public TableInputMeta(TableInputMeta m) {
    this();
    this.connection = m.connection;
    this.executeEachInputRow = m.executeEachInputRow;
    this.lookup = m.lookup;
    this.rowLimit = m.rowLimit;
    this.sql = m.sql;
    this.sqlFromFile = m.sqlFromFile;
    this.variableReplacementActive = m.variableReplacementActive;
  }

  @Override
  public Object clone() {
    return new TableInputMeta(this);
  }

  @Override
  public void setDefault() {
    sql = "SELECT <values> FROM <table name> WHERE <conditions>";
    rowLimit = "0";
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

    boolean param = false;

    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection),
          e);
    }

    Database db = new Database(loggingObject, variables, databaseMeta);
    super.databases = new Database[] {db}; // keep track of it for canceling purposes...

    // First try without connecting to the database... (can be S L O W)
    String effectiveSql;
    try {
      effectiveSql = getEffectiveSql(variables);
    } catch (HopException e) {
      throw new HopTransformException(e.getMessage(), e);
    }
    String sNewSql = effectiveSql;
    if (isVariableReplacementActive()) {
      sNewSql = db.resolve(effectiveSql);
      if (variables != null) {
        sNewSql = variables.resolve(sNewSql);
      }
    }

    IRowMeta add = null;
    try {
      add = db.getQueryFields(sNewSql, param);
    } catch (HopDatabaseException dbe) {
      throw new HopTransformException(
          "Unable to get queryfields for SQL: " + Const.CR + sNewSql, dbe);
    }

    if (add != null) {
      for (int i = 0; i < add.size(); i++) {
        IValueMeta v = add.getValueMeta(i);
        v.setOrigin(origin);
      }
      row.addRowMeta(add);
    } else {
      try {
        db.connect();

        IRowMeta paramRowMeta = null;
        Object[] paramData = null;

        IStream infoStream = getTransformIOMeta().getInfoStreams().get(0);
        if (!Utils.isEmpty(infoStream.getTransformName())) {
          param = true;
          if (info.length > 0 && info[0] != null) {
            paramRowMeta = info[0];
            paramData = RowDataUtil.allocateRowData(paramRowMeta.size());
          }
        }

        add = db.getQueryFields(sNewSql, param, paramRowMeta, paramData);

        if (add == null) {
          return;
        }
        for (int i = 0; i < add.size(); i++) {
          IValueMeta v = add.getValueMeta(i);
          v.setOrigin(origin);
        }
        row.addRowMeta(add);
      } catch (HopException ke) {
        throw new HopTransformException(
            "Unable to get queryfields for SQL: " + Const.CR + sNewSql, ke);
      } finally {
        db.disconnect();
      }
    }
  }

  /**
   * Returns the SQL to execute: either from the inline editor or loaded from the file specified by
   * sqlFromFile (using VFS). Variables are resolved in the file path.
   */
  public String getEffectiveSql(IVariables variables) throws HopException {
    if (!Utils.isEmpty(sqlFromFile)) {
      String path = variables.resolve(sqlFromFile);
      try {
        return HopVfs.getTextFileContent(path, StandardCharsets.UTF_8);
      } catch (HopFileException e) {
        throw new HopException(
            BaseMessages.getString(PKG, "TableInputMeta.Exception.CouldNotLoadSqlFromFile", path),
            e);
      }
    }
    return sql;
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

    String effectiveSql = null;
    try {
      effectiveSql = getEffectiveSql(variables);
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "Could not get SQL: " + e.getMessage(),
              transformMeta);
      remarks.add(cr);
    }

    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "TableInputMeta.CheckResult.DatabaseMetaError",
                  variables.resolve(connection)),
              transformMeta);
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, "Connection exists", transformMeta);
      remarks.add(cr);

      Database db = new Database(loggingObject, variables, databaseMeta);
      super.databases = new Database[] {db}; // keep track of it for canceling purposes...

      try {
        db.connect();
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK, "Connection to database OK", transformMeta);
        remarks.add(cr);

        if (effectiveSql != null) {
          if (!Utils.isEmpty(effectiveSql)) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK, "SQL statement is entered", transformMeta);
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR, "SQL statement is missing.", transformMeta);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                "An error occurred: " + e.getMessage(),
                transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "Please select or create a connection to use",
              transformMeta);
      remarks.add(cr);
    }

    // See if we have an informative transform...
    IStream infoStream = getTransformIOMeta().getInfoStreams().get(0);
    if (!Utils.isEmpty(infoStream.getTransformName())) {
      boolean found = false;
      for (String s : input) {
        if (infoStream.getTransformName().equalsIgnoreCase(s)) {
          found = true;
        }
      }
      if (found) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                "Previous transform to read info from ["
                    + infoStream.getTransformName()
                    + "] is found.",
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                "Previous transform to read info from ["
                    + infoStream.getTransformName()
                    + "] is not found.",
                transformMeta);
        remarks.add(cr);
      }

      // Count the number of ? in the SQL string:
      int count = 0;
      String sqlForParams = (effectiveSql != null) ? effectiveSql : "";
      for (int i = 0; i < sqlForParams.length(); i++) {
        char c = sqlForParams.charAt(i);
        if (c == '\'') { // skip to next quote!
          do {
            i++;
            c = sqlForParams.charAt(i);
          } while (c != '\'');
        }
        if (c == '?') {
          count++;
        }
      }
      // Verify with the number of informative fields...
      if (info != null) {
        if (count == info.size()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  "This transform is expecting and receiving "
                      + info.size()
                      + " fields of input from the previous transform.",
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  "This transform is receiving "
                      + info.size()
                      + " but not the expected "
                      + count
                      + " fields of input from the previous transform.",
                  transformMeta);
          remarks.add(cr);
        }
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                "Input transform name is not recognized!",
                transformMeta);
        remarks.add(cr);
      }
    } else {
      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                "Transform is not expecting info from input transforms.",
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                "No input expected, no input provided.",
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  /**
   * @param transforms optionally search the info transform in a list of transforms
   */
  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(TransformMeta.findTransform(transforms, stream.getSubject()));
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

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
      String effectiveSql = getEffectiveSql(variables);

      // Find the lookup fields.
      IRowMeta out = new RowMeta();

      getFields(
          out, transformMeta.getName(), new IRowMeta[] {info}, null, variables, metadataProvider);

      if (out != null) {
        for (int i = 0; i < out.size(); i++) {
          IValueMeta outvalue = out.getValueMeta(i);
          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_READ,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  "",
                  outvalue.getName(),
                  outvalue.getName(),
                  transformMeta.getName(),
                  effectiveSql,
                  "read from one or more database tables via SQL statement");
          impact.add(ii);
        }
      }
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection),
          e);
    }
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces
   * output, does not accept input!
   */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      IStream stream =
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "TableInputMeta.InfoStream.Description"),
              StreamIcon.INFO,
              lookup);
      ioMeta.addStream(stream);
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }
}
