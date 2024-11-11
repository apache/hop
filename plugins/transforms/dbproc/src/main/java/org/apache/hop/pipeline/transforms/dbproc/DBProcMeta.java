/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.dbproc;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "DBProc",
    image = "dbproc.svg",
    name = "i18n::CallDBProcedure.Name",
    description = "i18n::CallDBProcedure.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::DBProcMeta.keyword",
    documentationUrl = "/pipeline/transforms/calldbproc.html",
    actionTransformTypes = ActionTransformType.RDBMS)
public class DBProcMeta extends BaseTransformMeta<DBProc, DBProcData> {
  private static final Class<?> PKG = DBProcMeta.class;

  /** database connection */
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  /** procedure name to be called */
  @HopMetadataProperty private String procedure;

  /** function arguments */
  @HopMetadataProperty(groupKey = "lookup", key = "arg")
  List<ProcArgument> arguments;

  @HopMetadataProperty private ProcResult result;

  /** The flag to set auto commit on or off on the connection */
  @HopMetadataProperty(key = "auto_commit")
  private boolean autoCommit;

  public DBProcMeta() {
    super();
    this.arguments = new ArrayList<>();
    this.result = new ProcResult();
  }

  public DBProcMeta(DBProcMeta m) {
    this();
    this.connection = m.connection;
    this.procedure = m.procedure;
    for (ProcArgument argument : m.arguments) {
      this.arguments.add(new ProcArgument(argument));
    }
    this.result = new ProcResult(m.result);
    this.autoCommit = m.autoCommit;
  }

  @Override
  public DBProcMeta clone() {
    return new DBProcMeta(this);
  }

  @Override
  public void setDefault() {
    connection = null;
    result.name = "result";
    result.type = "Number";
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

    if (!Utils.isEmpty(result.getName())) {
      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta(result.getName(), result.getHopType());
        v.setOrigin(name);
        r.addValueMeta(v);
      } catch (HopPluginException e) {
        throw new HopTransformException(e);
      }
    }

    for (int i = 0; i < arguments.size(); i++) {
      ProcArgument argument = arguments.get(i);

      if (argument.getDirection().equalsIgnoreCase("OUT")) {
        IValueMeta v;
        try {
          v = ValueMetaFactory.createValueMeta(argument.getName(), argument.getHopType());
          v.setOrigin(name);
          r.addValueMeta(v);
        } catch (HopPluginException e) {
          throw new HopTransformException(e);
        }
      }
    }
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

    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "DBProcMeta.CheckResult.DatabaseMetaError", variables.resolve(connection)),
              transformMeta);
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        db.connect();

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < arguments.size(); i++) {
            ProcArgument argument = arguments.get(i);

            IValueMeta v = prev.searchValueMeta(argument.getName());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "DBProcMeta.CheckResult.MissingArguments")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + argument.getName() + Const.CR;
            } else {
              // Argument exists in input stream: same type?
              int hopType = argument.getHopType();
              if (v.getType() != hopType && !(v.isNumeric() && ValueMetaBase.isNumeric(hopType))) {
                errorFound = true;
                errorMessage +=
                    "\t\t"
                        + argument.getName()
                        + BaseMessages.getString(
                            PKG,
                            "DBProcMeta.CheckResult.WrongTypeArguments",
                            v.getTypeDesc(),
                            ValueMetaFactory.getValueMetaName(hopType))
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

  public String[] argumentNames() {
    String[] names = new String[arguments.size()];
    for (int i = 0; i < names.length; i++) {
      names[i] = arguments.get(i).getName();
    }
    return names;
  }

  public String[] argumentDirections() {
    String[] directions = new String[arguments.size()];
    for (int i = 0; i < directions.length; i++) {
      directions[i] = arguments.get(i).getDirection();
    }
    return directions;
  }

  public int[] argumentTypes() {
    int[] types = new int[arguments.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = arguments.get(i).getHopType();
    }
    return types;
  }

  public static class ProcArgument {
    @HopMetadataProperty private String name;
    @HopMetadataProperty private String direction;
    @HopMetadataProperty private String type;

    public ProcArgument() {}

    public ProcArgument(ProcArgument a) {
      this.name = a.name;
      this.direction = a.direction;
      this.type = a.type;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets direction
     *
     * @return value of direction
     */
    public String getDirection() {
      return direction;
    }

    /**
     * Sets direction
     *
     * @param direction value of direction
     */
    public void setDirection(String direction) {
      this.direction = direction;
    }

    /**
     * Gets type
     *
     * @return value of type
     */
    public String getType() {
      return type;
    }

    /**
     * Sets type
     *
     * @param type value of type
     */
    public void setType(String type) {
      this.type = type;
    }

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(type);
    }
  }

  public static class ProcResult {
    /** function result: new value name */
    @HopMetadataProperty private String name;

    /** function result: new value type */
    @HopMetadataProperty private String type;

    public ProcResult() {}

    public ProcResult(ProcResult r) {
      this.name = r.name;
      this.type = r.type;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets type
     *
     * @return value of type
     */
    public String getType() {
      return type;
    }

    /**
     * Sets type
     *
     * @param type value of type
     */
    public void setType(String type) {
      this.type = type;
    }

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(type);
    }
  }

  /**
   * Gets database connection
   *
   * @return value of connection
   */
  public String getConnection() {
    return connection;
  }

  /**
   * Sets database connection
   *
   * @param connection value of database connection
   */
  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * Gets procedure
   *
   * @return value of procedure
   */
  public String getProcedure() {
    return procedure;
  }

  /**
   * Sets procedure
   *
   * @param procedure value of procedure
   */
  public void setProcedure(String procedure) {
    this.procedure = procedure;
  }

  /**
   * Gets arguments
   *
   * @return value of arguments
   */
  public List<ProcArgument> getArguments() {
    return arguments;
  }

  /**
   * Sets arguments
   *
   * @param arguments value of arguments
   */
  public void setArguments(List<ProcArgument> arguments) {
    this.arguments = arguments;
  }

  /**
   * Gets result
   *
   * @return value of result
   */
  public ProcResult getResult() {
    return result;
  }

  /**
   * Sets result
   *
   * @param result value of result
   */
  public void setResult(ProcResult result) {
    this.result = result;
  }

  /**
   * Gets autoCommit
   *
   * @return value of autoCommit
   */
  public boolean isAutoCommit() {
    return autoCommit;
  }

  /**
   * Sets autoCommit
   *
   * @param autoCommit value of autoCommit
   */
  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }
}
