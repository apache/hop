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

package org.apache.hop.pipeline.transforms.databasejoin;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNone;
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
    id = "DBJoin",
    image = "dbjoin.svg",
    name = "i18n::BaseTransform.TypeLongDesc.DatabaseJoin",
    description = "i18n::BaseTransform.TypeTooltipDesc.DatabaseJoin",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/databasejoin.html")
public class DatabaseJoinMeta extends BaseTransformMeta
    implements ITransformMeta<DatabaseJoin, DatabaseJoinData> {

  private static final Class<?> PKG = DatabaseJoinMeta.class; // For Translator

  /** database connection */
  private DatabaseMeta databaseMeta;

  /** SQL Statement */
  private String sql;

  /** Number of rows to return (0=ALL) */
  private int rowLimit;

  /**
   * false: don't return rows where nothing is found true: at least return one source row, the rest
   * is NULL
   */
  private boolean outerJoin;

  /** Fields to use as parameters (fill in the ? markers) */
  private String[] parameterField;

  /** Type of the paramenters */
  private int[] parameterType;

  /** false: don't replave variable in scrip true: replace variable in script */
  private boolean replacevars;

  public DatabaseJoinMeta() {
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

  /** @return Returns the parameterField. */
  public String[] getParameterField() {
    return parameterField;
  }

  /** @param parameterField The parameterField to set. */
  public void setParameterField(String[] parameterField) {
    this.parameterField = parameterField;
  }

  /** @return Returns the parameterType. */
  public int[] getParameterType() {
    return parameterType;
  }

  /** @param parameterType The parameterType to set. */
  public void setParameterType(int[] parameterType) {
    this.parameterType = parameterType;
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

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    parameterField = null;
    parameterType = null;
    outerJoin = false;
    replacevars = false;
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrparam) {
    parameterField = new String[nrparam];
    parameterType = new int[nrparam];
  }

  @Override
  public Object clone() {
    DatabaseJoinMeta retval = (DatabaseJoinMeta) super.clone();

    int nrparam = parameterField.length;

    retval.allocate(nrparam);

    System.arraycopy(parameterField, 0, retval.parameterField, 0, nrparam);
    System.arraycopy(parameterType, 0, retval.parameterType, 0, nrparam);

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
      rowLimit = Const.toInt(XmlHandler.getTagValue(transformNode, "rowlimit"), 0);

      Node param = XmlHandler.getSubNode(transformNode, "parameter");
      int nrparam = XmlHandler.countNodes(param, "field");

      allocate(nrparam);

      for (int i = 0; i < nrparam; i++) {
        Node pnode = XmlHandler.getSubNodeByNr(param, "field", i);
        parameterField[i] = XmlHandler.getTagValue(pnode, "name");
        String ptype = XmlHandler.getTagValue(pnode, "type");
        parameterType[i] = ValueMetaFactory.getIdForValueMeta(ptype);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "DatabaseJoinMeta.Exception.UnableToLoadTransformMeta"), e);
    }
  }

  @Override
  public void setDefault() {
    databaseMeta = null;
    rowLimit = 0;
    sql = "";
    outerJoin = false;
    parameterField = null;
    parameterType = null;
    outerJoin = false;
    replacevars = false;

    int nrparam = 0;

    allocate(nrparam);

    for (int i = 0; i < nrparam; i++) {
      parameterField[i] = "param" + i;
      parameterType[i] = IValueMeta.TYPE_NUMBER;
    }
  }

  public IRowMeta getParameterRow(IRowMeta fields) {
    IRowMeta param = new RowMeta();

    if (fields != null) {
      for (int i = 0; i < parameterField.length; i++) {
        IValueMeta v = fields.searchValueMeta(parameterField[i]);
        if (v != null) {
          param.addValueMeta(v);
        }
      }
    }
    return param;
  }

  @Override
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

    // Which fields are parameters?
    // info[0] comes from the database connection.
    //
    IRowMeta param = getParameterRow(row);

    // First try without connecting to the database... (can be S L O W)
    // See if it's in the cache...
    //
    IRowMeta add = null;
    try {
      add = db.getQueryFields(variables.resolve(sql), true, param, new Object[param.size()]);
    } catch (HopDatabaseException dbe) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DatabaseJoinMeta.Exception.UnableToDetermineQueryFields")
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
      //
      try {
        db.connect();
        add = db.getQueryFields(variables.resolve(sql), true, param, new Object[param.size()]);
        for (int i = 0; i < add.size(); i++) {
          IValueMeta v = add.getValueMeta(i);
          v.setOrigin(name);
        }
        row.addRowMeta(add);
        db.disconnect();
      } catch (HopDatabaseException dbe) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "DatabaseJoinMeta.Exception.ErrorObtainingFields"), dbe);
      }
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("    ").append(XmlHandler.addTagValue("rowlimit", rowLimit));
    retval.append("    ").append(XmlHandler.addTagValue("sql", sql));
    retval.append("    ").append(XmlHandler.addTagValue("outer_join", outerJoin));
    retval.append("    ").append(XmlHandler.addTagValue("replace_vars", replacevars));
    retval.append("    <parameter>").append(Const.CR);
    for (int i = 0; i < parameterField.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", parameterField[i]));
      retval
          .append("        ")
          .append(
              XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(parameterType[i])));
      retval.append("      </field>").append(Const.CR);
    }
    retval.append("    </parameter>").append(Const.CR);

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
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        db.connect();
        if (sql != null && sql.length() != 0) {
          IRowMeta param = getParameterRow(prev);

          errorMessage = "";

          IRowMeta r =
              db.getQueryFields(variables.resolve(sql), true, param, new Object[param.size()]);
          if (r != null) {
            cr =
                new CheckResult(
                    CheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.QueryOK"),
                    transformMeta);
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.InvalidDBQuery");
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }

          int q = db.countParameters(variables.resolve(sql));
          if (q != parameterField.length) {
            errorMessage =
                BaseMessages.getString(
                        PKG, "DatabaseJoinMeta.CheckResult.DismatchBetweenParametersAndQuestion")
                    + Const.CR;
            errorMessage +=
                BaseMessages.getString(
                        PKG, "DatabaseJoinMeta.CheckResult.DismatchBetweenParametersAndQuestion2")
                    + q
                    + Const.CR;
            errorMessage +=
                BaseMessages.getString(
                        PKG, "DatabaseJoinMeta.CheckResult.DismatchBetweenParametersAndQuestion3")
                    + parameterField.length;

            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    CheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.NumberOfParamCorrect")
                        + q
                        + ")",
                    transformMeta);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < parameterField.length; i++) {
            IValueMeta v = prev.searchValueMeta(parameterField[i]);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.MissingFields")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + parameterField[i] + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    CheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.AllFieldsFound"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.CounldNotReadFields")
                  + Const.CR;
          cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.ErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.ReceivingInfo"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public IRowMeta getTableFields(IVariables variables) {
    // Build a dummy parameter row...
    //
    IRowMeta param = new RowMeta();
    for (int i = 0; i < parameterField.length; i++) {
      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta(parameterField[i], parameterType[i]);
      } catch (HopPluginException e) {
        v = new ValueMetaNone(parameterField[i]);
      }
      param.addValueMeta(v);
    }

    IRowMeta fields = null;
    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta );
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        db.connect();
        fields = db.getQueryFields(variables.resolve(sql), true, param, new Object[param.size()]);
      } catch (HopDatabaseException dbe) {
        logError(
            BaseMessages.getString(PKG, "DatabaseJoinMeta.Log.DatabaseErrorOccurred")
                + dbe.getMessage());
      } finally {
        db.disconnect();
      }
    }
    return fields;
  }

  @Override
  public DatabaseJoin createTransform(
      TransformMeta transformMeta,
      DatabaseJoinData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new DatabaseJoin(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public DatabaseJoinData getTransformData() {
    return new DatabaseJoinData();
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

    // Find the lookupfields...
    //
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
                variables.resolve(sql),
                BaseMessages.getString(PKG, "DatabaseJoinMeta.DatabaseImpact.Title"));
        impact.add(di);
      }
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
  public boolean supportsErrorHandling() {
    return true;
  }
}
