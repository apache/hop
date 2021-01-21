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

package org.apache.hop.pipeline.transforms.dbproc;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 26-apr-2003
 *
 */

@Transform(
    id = "DBProc",
    image = "dbproc.svg",
    name = "i18n::BaseTransform.TypeLongDesc.CallDBProcedure",
    description = "i18n::BaseTransform.TypeTooltipDesc.CallDBProcedure",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/dbproc.html")
public class DBProcMeta extends BaseTransformMeta implements ITransformMeta<DBProc, DBProcData> {

  private static final Class<?> PKG = DBProcMeta.class; // For Translator

  /** database connection */
  private DatabaseMeta database;

  /** proc.-name to be called */
  private String procedure;

  /** function arguments */
  private String[] argument;

  /** IN / OUT / INOUT */
  private String[] argumentDirection;

  /** value type for OUT */
  private int[] argumentType;

  /** function result: new value name */
  private String resultName;

  /** function result: new value type */
  private int resultType;

  /** The flag to set auto commit on or off on the connection */
  private boolean autoCommit;

  public DBProcMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the argument. */
  public String[] getArgument() {
    return argument;
  }

  /** @param argument The argument to set. */
  public void setArgument(String[] argument) {
    this.argument = argument;
  }

  /** @return Returns the argumentDirection. */
  public String[] getArgumentDirection() {
    return argumentDirection;
  }

  /** @param argumentDirection The argumentDirection to set. */
  public void setArgumentDirection(String[] argumentDirection) {
    this.argumentDirection = argumentDirection;
  }

  /** @return Returns the argumentType. */
  public int[] getArgumentType() {
    return argumentType;
  }

  /** @param argumentType The argumentType to set. */
  public void setArgumentType(int[] argumentType) {
    this.argumentType = argumentType;
  }

  /** @return Returns the database. */
  public DatabaseMeta getDatabase() {
    return database;
  }

  /** @param database The database to set. */
  public void setDatabase(DatabaseMeta database) {
    this.database = database;
  }

  /** @return Returns the procedure. */
  public String getProcedure() {
    return procedure;
  }

  /** @param procedure The procedure to set. */
  public void setProcedure(String procedure) {
    this.procedure = procedure;
  }

  /** @return Returns the resultName. */
  public String getResultName() {
    return resultName;
  }

  /** @param resultName The resultName to set. */
  public void setResultName(String resultName) {
    this.resultName = resultName;
  }

  /** @return Returns the resultType. */
  public int getResultType() {
    return resultType;
  }

  /** @param resultType The resultType to set. */
  public void setResultType(int resultType) {
    this.resultType = resultType;
  }

  /** @return Returns the autoCommit. */
  public boolean isAutoCommit() {
    return autoCommit;
  }

  /** @param autoCommit The autoCommit to set. */
  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrargs) {
    argument = new String[nrargs];
    argumentDirection = new String[nrargs];
    argumentType = new int[nrargs];
  }

  public Object clone() {
    DBProcMeta retval = (DBProcMeta) super.clone();
    int nrargs = argument.length;

    retval.allocate(nrargs);

    System.arraycopy(argument, 0, retval.argument, 0, nrargs);
    System.arraycopy(argumentDirection, 0, retval.argumentDirection, 0, nrargs);
    System.arraycopy(argumentType, 0, retval.argumentType, 0, nrargs);

    return retval;
  }

  public void setDefault() {
    int i;
    int nrargs;

    database = null;

    nrargs = 0;

    allocate(nrargs);

    for (i = 0; i < nrargs; i++) {
      argument[i] = "arg" + i;
      argumentDirection[i] = "IN";
      argumentType[i] = IValueMeta.TYPE_NUMBER;
    }

    resultName = "result";
    resultType = IValueMeta.TYPE_NUMBER;
    autoCommit = true;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (!Utils.isEmpty(resultName)) {
      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta(resultName, resultType);
        v.setOrigin(name);
        r.addValueMeta(v);
      } catch (HopPluginException e) {
        throw new HopTransformException(e);
      }
    }

    for (int i = 0; i < argument.length; i++) {
      if (argumentDirection[i].equalsIgnoreCase("OUT")) {
        IValueMeta v;
        try {
          v = ValueMetaFactory.createValueMeta(argument[i], argumentType[i]);
          v.setOrigin(name);
          r.addValueMeta(v);
        } catch (HopPluginException e) {
          throw new HopTransformException(e);
        }
      }
    }

    return;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

    retval
        .append("    ")
        .append(XmlHandler.addTagValue("connection", database == null ? "" : database.getName()));
    retval.append("    ").append(XmlHandler.addTagValue("procedure", procedure));
    retval.append("    <lookup>").append(Const.CR);

    for (int i = 0; i < argument.length; i++) {
      retval.append("      <arg>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", argument[i]));
      retval.append("        ").append(XmlHandler.addTagValue("direction", argumentDirection[i]));
      retval
          .append("        ")
          .append(
              XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(argumentType[i])));
      retval.append("      </arg>").append(Const.CR);
    }

    retval.append("    </lookup>").append(Const.CR);

    retval.append("    <result>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("name", resultName));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(resultType)));
    retval.append("    </result>").append(Const.CR);

    retval.append("    ").append(XmlHandler.addTagValue("auto_commit", autoCommit));

    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      int i;
      int nrargs;

      String con = XmlHandler.getTagValue(transformNode, "connection");
      database = DatabaseMeta.loadDatabase(metadataProvider, con);
      procedure = XmlHandler.getTagValue(transformNode, "procedure");

      Node lookup = XmlHandler.getSubNode(transformNode, "lookup");
      nrargs = XmlHandler.countNodes(lookup, "arg");

      allocate(nrargs);

      for (i = 0; i < nrargs; i++) {
        Node anode = XmlHandler.getSubNodeByNr(lookup, "arg", i);

        argument[i] = XmlHandler.getTagValue(anode, "name");
        argumentDirection[i] = XmlHandler.getTagValue(anode, "direction");
        argumentType[i] = ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(anode, "type"));
      }

      resultName = XmlHandler.getTagValue(transformNode, "result", "name"); // Optional, can be null
      //
      resultType =
          ValueMetaFactory.getIdForValueMeta(
              XmlHandler.getTagValue(transformNode, "result", "type"));
      autoCommit = !"N".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "auto_commit"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "DBProcMeta.Exception.UnableToReadTransformMeta"), e);
    }
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

    if (database != null) {
      Database db = new Database(pipelineMeta, variables, database );
      try {
        db.connect();

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < argument.length; i++) {
            IValueMeta v = prev.searchValueMeta(argument[i]);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "DBProcMeta.CheckResult.MissingArguments")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + argument[i] + Const.CR;
            } else {
              // Argument exists in input stream: same type?

              if (v.getType() != argumentType[i]
                  && !(v.isNumeric() && ValueMetaBase.isNumeric(argumentType[i]))) {
                errorFound = true;
                errorMessage +=
                    "\t\t"
                        + argument[i]
                        + BaseMessages.getString(
                            PKG,
                            "DBProcMeta.CheckResult.WrongTypeArguments",
                            v.getTypeDesc(),
                            ValueMetaFactory.getValueMetaName(argumentType[i]))
                        + Const.CR;
              }
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DBProcMeta.CheckResult.AllArgumentsOK"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "DBProcMeta.CheckResult.CouldNotReadFields") + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "DBProcMeta.CheckResult.ErrorOccurred") + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "DBProcMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "DBProcMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DBProcMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public DBProc createTransform(
      TransformMeta transformMeta,
      DBProcData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new DBProc(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public DBProcData getTransformData() {
    return new DBProcData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (database != null) {
      return new DatabaseMeta[] {database};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
