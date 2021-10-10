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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "TableInput",
    image = "tableinput.svg",
    name = "i18n::TableInput.Name",
    description = "i18n::TableInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/tableinput.html",
    keywords = "input, sql")
public class TableInputMeta extends BaseTransformMeta
    implements ITransformMeta<TableInput, TableInputData> {

  private static final Class<?> PKG = TableInputMeta.class; // For Translator

  @HopMetadataProperty(key = "sql", injectionKey = "SQL")
  private String sql;

  @HopMetadataProperty(key = "limit", injectionKey = "LIMIT")
  private String rowLimit;

  /** Should I execute once per row? */
  @HopMetadataProperty(key = "execute_each_row", injectionKey = "EXECUTE_FOR_EACH_ROW")
  private boolean executeEachInputRow;

  @HopMetadataProperty(key = "variables_active", injectionKey = "REPLACE_VARIABLES")
  private boolean variableReplacementActive;

  @HopMetadataProperty(key = "connection", injectionKey = "CONNECTIONNAME")
  private String connection;

  @HopMetadataProperty private String lookup;

  public TableInputMeta() {
    super();
  }

  /** @return Returns true if the transform should be run per row */
  public boolean isExecuteEachInputRow() {
    return executeEachInputRow;
  }

  /** @param oncePerRow true if the transform should be run per row */
  public void setExecuteEachInputRow(boolean oncePerRow) {
    this.executeEachInputRow = oncePerRow;
  }

  /** @return Returns the rowLimit. */
  public String getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(String rowLimit) {
    this.rowLimit = rowLimit;
  }

  /** @return Returns the sql. */
  public String getSql() {
    return sql;
  }

  /** @param sql The sql to set. */
  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  public String getLookup() {
    return lookup;
  }

  public void setLookup(String lookup) {
    this.lookup = lookup;
  }

  @Override
  public Object clone() {
    TableInputMeta retval = (TableInputMeta) super.clone();
    return retval;
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
    String sNewSql = sql;
    if (isVariableReplacementActive()) {
      sNewSql = db.resolve(sql);
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

  @Override
  public String getXml() throws HopException {

    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    lookup = infoStreams.get(0).getTransformName();

    return super.getXml();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);

    IStream infoStream = getTransformIOMeta().getInfoStreams().get(0);
    infoStream.setSubject(lookup);
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

        if (sql != null && sql.length() != 0) {
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
      for (int i = 0; i < input.length; i++) {
        if (infoStream.getTransformName().equalsIgnoreCase(input[i])) {
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
      for (int i = 0; i < sql.length(); i++) {
        char c = sql.charAt(i);
        if (c == '\'') { // skip to next quote!
          do {
            i++;
            c = sql.charAt(i);
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

  /** @param transforms optionally search the info transform in a list of transforms */
  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(
          TransformMeta.findTransform(transforms, (String) stream.getSubject()));
    }
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      TableInputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new TableInput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public TableInputData getTransformData() {
    return new TableInputData();
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

      // Find the lookupfields...
      IRowMeta out = new RowMeta();
      // TODO: this builds, but does it work in all cases.
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
                  sql,
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

  /** @return Returns the variableReplacementActive. */
  public boolean isVariableReplacementActive() {
    return variableReplacementActive;
  }

  /** @param variableReplacementActive The variableReplacementActive to set. */
  public void setVariableReplacementActive(boolean variableReplacementActive) {
    this.variableReplacementActive = variableReplacementActive;
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
              null);
      ioMeta.addStream(stream);
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
    // Do nothing, don't reset as there is no need to do this.
  }

  /**
   * For compatibility, wraps around the standard transform IO metadata
   *
   * @param transformMeta The transform where you read lookup data from
   */
  public void setLookupFromTransform(TransformMeta transformMeta) {
    getTransformIOMeta().getInfoStreams().get(0).setTransformMeta(transformMeta);
  }

  /**
   * For compatibility, wraps around the standard transform IO metadata
   *
   * @return The transform where you read lookup data from
   */
  public TransformMeta getLookupFromTransform() {
    return getTransformIOMeta().getInfoStreams().get(0).getTransformMeta();
  }
}
