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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
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
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
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
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::DBProcMeta.keyword",
    documentationUrl = "/pipeline/transforms/calldbproc.html",
    actionTransformTypes = ActionTransformType.RDBMS)
@GuiPlugin
@Getter
@Setter
public class DBProcMeta extends BaseTransformMeta<DBProc, DBProcData> {
  private static final Class<?> PKG = DBProcMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DBProcDialog";
  public static final String WIDGET_CONNECTION = "connection";
  public static final String WIDGET_PROCEDURE = "procedure";
  public static final String WIDGET_AUTO_COMMIT = "autoCommit";
  public static final String WIDGET_RESULT_NAME = "resultName";
  public static final String WIDGET_RESULT_TYPE = "resultType";

  /** Result type that outputs the rows of the procedure's first result set. */
  public static final String RESULT_TYPE_ROW = "Row";

  public static final String GROUP_GENERAL = "i18n::DBProcDialog.Group.General";

  /** database connection */
  @GuiWidgetElement(
      id = WIDGET_CONNECTION,
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      label = "i18n::DBProcDialog.Connection.Label",
      toolTip = "i18n::DBProcDialog.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  /** procedure name to be called */
  @GuiWidgetElement(
      id = WIDGET_PROCEDURE,
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::DBProcDialog.ProcedureName.Label",
      toolTip = "i18n::DBProcDialog.ProcedureName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty
  private String procedure;

  /** The flag to set auto commit on or off on the connection */
  @GuiWidgetElement(
      id = WIDGET_AUTO_COMMIT,
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::DBProcDialog.AutoCommit.Label",
      toolTip = "i18n::DBProcDialog.AutoCommit.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS,
      getterMethod = "isAutoCommit")
  @HopMetadataProperty(key = "auto_commit")
  private boolean autoCommit;

  /** Not stored. The dialog edits {@link ProcResult#name} through this widget. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  @GuiWidgetElement(
      id = WIDGET_RESULT_NAME,
      order = "0400",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::DBProcDialog.Result.Label",
      toolTip = "i18n::DBProcDialog.Result.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS,
      getterMethod = "getResultName",
      setterMethod = "setResultName")
  private String resultNameForUi;

  /** Not stored. The dialog edits {@link ProcResult#type} through this widget. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  @GuiWidgetElement(
      id = WIDGET_RESULT_TYPE,
      order = "0500",
      type = GuiElementType.COMBO,
      variables = false,
      comboValuesMethod = "getResultTypeNames",
      label = "i18n::DBProcDialog.ResultType.Label",
      toolTip = "i18n::DBProcDialog.ResultType.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS,
      getterMethod = "getResultType",
      setterMethod = "setResultType")
  private String resultTypeForUi;

  /** function arguments */
  @HopMetadataProperty(groupKey = "lookup", key = "arg")
  private List<ProcArgument> arguments;

  @HopMetadataProperty private ProcResult result;

  /** Columns of the first result set when {@link #result} type is {@link #RESULT_TYPE_ROW}. */
  @HopMetadataProperty(groupKey = "result_fields", key = "field")
  private List<DBProcField> resultFields;

  public DBProcMeta() {
    super();
    this.arguments = new ArrayList<>();
    this.resultFields = new ArrayList<>();
    this.result = new ProcResult();
  }

  @Override
  public void setDefault() {
    connection = null;
    result.setName("result");
    result.setType("Number");
    autoCommit = true;
  }

  public boolean isResultRows() {
    return RESULT_TYPE_ROW.equalsIgnoreCase(getResultType());
  }

  public String getResultName() {
    return result == null ? null : result.getName();
  }

  public void setResultName(String name) {
    ensureResult();
    result.setName(name);
  }

  public String getResultType() {
    return result == null ? null : result.getType();
  }

  public void setResultType(String type) {
    ensureResult();
    result.setType(type);
  }

  private void ensureResult() {
    if (result == null) {
      result = new ProcResult();
    }
  }

  public List<String> getResultTypeNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> names = new ArrayList<>();
    for (String name : ValueMetaFactory.getValueMetaNames()) {
      if (!RESULT_TYPE_ROW.equalsIgnoreCase(name)) {
        names.add(name);
      }
    }
    names.add(RESULT_TYPE_ROW);
    return names;
  }

  /** Result columns with a name. Empty names are not part of the output row. */
  public List<DBProcField> activeResultFields() {
    List<DBProcField> active = new ArrayList<>();
    if (resultFields == null) {
      return active;
    }
    for (DBProcField field : resultFields) {
      if (field != null && !Utils.isEmpty(field.getName())) {
        active.add(field);
      }
    }
    return active;
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

    if (isResultRows()) {
      for (DBProcField field : activeResultFields()) {
        try {
          r.addValueMeta(field.toValueMeta(name, variables));
        } catch (HopPluginException e) {
          throw new HopTransformException(e);
        }
      }
    } else if (result != null && !Utils.isEmpty(result.getName())) {
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(result.getName(), result.getHopType());
        v.setOrigin(name);
        r.addValueMeta(v);
      } catch (HopPluginException e) {
        throw new HopTransformException(e);
      }
    }

    if (arguments == null) {
      return;
    }
    for (ProcArgument argument : arguments) {
      if (argument.getDirection() != null && argument.getDirection().equalsIgnoreCase("OUT")) {
        try {
          IValueMeta v =
              ValueMetaFactory.createValueMeta(argument.getName(), argument.getHopType());
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
        if (prev != null && !prev.isEmpty()) {
          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (ProcArgument argument : arguments) {
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
    if (arguments == null) {
      return new String[0];
    }
    String[] names = new String[arguments.size()];
    for (int i = 0; i < names.length; i++) {
      names[i] = arguments.get(i).getName();
    }
    return names;
  }

  public String[] argumentDirections() {
    if (arguments == null) {
      return new String[0];
    }
    String[] directions = new String[arguments.size()];
    for (int i = 0; i < directions.length; i++) {
      directions[i] = arguments.get(i).getDirection();
    }
    return directions;
  }

  public int[] argumentTypes() {
    if (arguments == null) {
      return new int[0];
    }
    int[] types = new int[arguments.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = arguments.get(i).getHopType();
    }
    return types;
  }

  @Getter
  @Setter
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

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(type);
    }
  }

  @Getter
  @Setter
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

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(type);
    }
  }
}
