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

package org.apache.hop.pipeline.transforms.dynamicsqlrow;

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
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "DynamicSqlRow",
    image = "dynamicsqlrow.svg",
    name = "i18n::BaseTransform.TypeLongDesc.DynamicSQLRow",
    description = "i18n::BaseTransform.TypeTooltipDesc.DynamicSQLRow",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/dynamicsqlrow.html")
public class DynamicSqlRowMeta extends BaseTransformMeta
    implements ITransformMeta<DynamicSqlRow, DynamicSqlRowData> {
  private static final Class<?> PKG = DynamicSqlRowMeta.class; // For Translator

  /** database connection */
  private DatabaseMeta databaseMeta;

  /** SQL Statement */
  private String sql;

  private String sqlfieldname;

  /** Number of rows to return (0=ALL) */
  private int rowLimit;

  /**
   * false: don't return rows where nothing is found true: at least return one source row, the rest
   * is NULL
   */
  private boolean outerJoin;

  private boolean replacevars;

  public boolean queryonlyonchange;

  public DynamicSqlRowMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the database. */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param database The database to set. */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /** @return Returns the outerJoin. */
  public boolean isOuterJoin() {
    return outerJoin;
  }

  /** @param outerJoin The outerJoin to set. */
  public void setOuterJoin(boolean outerJoin) {
    this.outerJoin = outerJoin;
  }

  /** @return Returns the replacevars. */
  public boolean isVariableReplace() {
    return replacevars;
  }

  /** @param replacevars The replacevars to set. */
  public void setVariableReplace(boolean replacevars) {
    this.replacevars = replacevars;
  }

  /** @return Returns the queryonlyonchange. */
  public boolean isQueryOnlyOnChange() {
    return queryonlyonchange;
  }

  /** @param queryonlyonchange The queryonlyonchange to set. */
  public void setQueryOnlyOnChange(boolean queryonlyonchange) {
    this.queryonlyonchange = queryonlyonchange;
  }

  /** @return Returns the rowLimit. */
  public int getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(int rowLimit) {
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

  /** @return Returns the sqlfieldname. */
  public String getSqlFieldName() {
    return sqlfieldname;
  }

  /** @param sqlfieldname The sqlfieldname to set. */
  public void setSqlFieldName(String sqlfieldname) {
    this.sqlfieldname = sqlfieldname;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    DynamicSqlRowMeta retval = (DynamicSqlRowMeta) super.clone();

    return retval;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      sql = XmlHandler.getTagValue(transformNode, "sql");
      outerJoin = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "outer_join"));
      replacevars = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "replace_vars"));
      queryonlyonchange =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "query_only_on_change"));

      rowLimit = Const.toInt(XmlHandler.getTagValue(transformNode, "rowlimit"), 0);
      sqlfieldname = XmlHandler.getTagValue(transformNode, "sql_fieldname");

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "DynamicSQLRowMeta.Exception.UnableToLoadTransformMeta"), e);
    }
  }

  public void setDefault() {
    databaseMeta = null;
    rowLimit = 0;
    sql = "";
    outerJoin = false;
    replacevars = false;
    sqlfieldname = null;
    queryonlyonchange = false;
  }

  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (databaseMeta == null) {
      return;
    }

    Database db = new Database(loggingObject, variables, databaseMeta );
    databases = new Database[] {db}; // Keep track of this one for cancelQuery

    // First try without connecting to the database... (can be S L O W)
    // See if it's in the cache...
    IRowMeta add = null;
    String realSql = sql;
    if (replacevars) {
      realSql = variables.resolve(realSql);
    }
    try {
      add = db.getQueryFields(realSql, false);
    } catch (HopDatabaseException dbe) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DynamicSQLRowMeta.Exception.UnableToDetermineQueryFields")
              + Const.CR
              + sql,
          dbe);
    }

    if (add != null) { // Cache hit, just return it this...
      for (int i = 0; i < add.size(); i++) {
        IValueMeta v = add.getValueMeta(i);
        v.setOrigin(name);
      }
      row.addRowMeta(add);
    } else {
      // No cache hit, connect to the database, do it the hard way...
      try {
        db.connect();
        add = db.getQueryFields(realSql, false);
        for (int i = 0; i < add.size(); i++) {
          IValueMeta v = add.getValueMeta(i);
          v.setOrigin(name);
        }
        row.addRowMeta(add);
        db.disconnect();
      } catch (HopDatabaseException dbe) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "DynamicSQLRowMeta.Exception.ErrorObtainingFields"), dbe);
      }
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("    " + XmlHandler.addTagValue("rowlimit", rowLimit));
    retval.append("    " + XmlHandler.addTagValue("sql", sql));
    retval.append("    " + XmlHandler.addTagValue("outer_join", outerJoin));
    retval.append("    " + XmlHandler.addTagValue("replace_vars", replacevars));
    retval.append("    " + XmlHandler.addTagValue("sql_fieldname", sqlfieldname));
    retval.append("    " + XmlHandler.addTagValue("query_only_on_change", queryonlyonchange));

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
    CheckResult cr;
    String errorMessage = "";

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.ReceivingInfo"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for SQL field
    if (Utils.isEmpty(sqlfieldname)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.SQLFieldNameMissing"),
              transformMeta);
      remarks.add(cr);
    } else {
      IValueMeta vfield = prev.searchValueMeta(sqlfieldname);
      if (vfield == null) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "DynamicSQLRowMeta.CheckResult.SQLFieldNotFound", sqlfieldname),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "DynamicSQLRowMeta.CheckResult.SQLFieldFound",
                    sqlfieldname,
                    vfield.getOrigin()),
                transformMeta);
      }
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta );
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        db.connect();
        if (sql != null && sql.length() != 0) {

          errorMessage = "";

          IRowMeta r = db.getQueryFields(sql, true);
          if (r != null) {
            cr =
                new CheckResult(
                    CheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.QueryOK"),
                    transformMeta);
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.InvalidDBQuery");
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.ErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
  }

  public DynamicSqlRow createTransform(
      TransformMeta transformMeta,
      DynamicSqlRowData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new DynamicSqlRow(transformMeta, this, data, cnr, tr, pipeline);
  }

  public DynamicSqlRowData getTransformData() {
    return new DynamicSqlRowData();
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
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    IRowMeta out = prev.clone();
    getFields(
        out,
        transformMeta.getName(),
        new IRowMeta[] {
          info,
        },
        null,
        variables,
        metadataProvider);
    if (out != null) {
      for (int i = 0; i < out.size(); i++) {
        IValueMeta outvalue = out.getValueMeta(i);
        DatabaseImpact di =
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
                BaseMessages.getString(PKG, "DynamicSQLRowMeta.DatabaseImpact.Title"));
        impact.add(di);
      }
    }
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
