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

package org.apache.hop.pipeline.transforms.sql;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
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
import org.apache.hop.pipeline.transform.TransformMeta;

/*
 * Contains meta-data to execute arbitrary SQL, optionally each row again.
 */

@Transform(
    id = "ExecSql",
    image = "sql.svg",
    name = "i18n::ExecSql.Name",
    description = "i18n::ExecSql.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::ExecSqlMeta.keyword",
    documentationUrl = "/pipeline/transforms/execsql.html",
    actionTransformTypes = {ActionTransformType.RDBMS})
@GuiPlugin
@Getter
@Setter
public class ExecSqlMeta extends BaseTransformMeta<ExecSql, ExecSqlData> {
  private static final Class<?> PKG = ExecSqlMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "EXEC_SQL_DIALOG_OPTIONS";
  public static final String WIDGET_CONNECTION = "CONNECTION";
  public static final String WIDGET_INSERT_FIELD = "INSERT_FIELD";
  public static final String WIDGET_UPDATE_FIELD = "UPDATE_FIELD";
  public static final String WIDGET_DELETE_FIELD = "DELETE_FIELD";
  public static final String WIDGET_READ_FIELD = "READ_FIELD";
  public static final String WIDGET_SQL_FROM_FILE = "SQL_FROM_FILE";
  public static final String WIDGET_BIND_PARAMETERS = "BIND_PARAMETERS";

  public static final String GROUP_GENERAL = "i18n::ExecSqlMeta.Group.General";
  public static final String GROUP_SQL = "i18n::ExecSqlMeta.Group.SQL";
  public static final String GROUP_PARAMETERS = "i18n::ExecSqlMeta.Group.Parameters";

  @GuiWidgetElement(
      id = WIDGET_CONNECTION,
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      label = "i18n::ExecSqlMeta.Connection.Label",
      toolTip = "i18n::ExecSqlMeta.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "ExecSqlMeta.Injection.CONNECTIONNAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @GuiWidgetElement(
      id = WIDGET_INSERT_FIELD,
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::ExecSqlDialog.InsertField.Label",
      toolTip = "i18n::ExecSqlMeta.Injection.INSERT_STATS_FIELD",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "insert_field",
      injectionKeyDescription = "ExecSqlMeta.Injection.INSERT_STATS_FIELD",
      injectionKey = "INSERT_STATS_FIELD")
  private String insertField;

  @GuiWidgetElement(
      id = WIDGET_UPDATE_FIELD,
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::ExecSqlDialog.UpdateField.Label",
      toolTip = "i18n::ExecSqlMeta.Injection.UPDATE_STATS_FIELD",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "update_field",
      injectionKeyDescription = "ExecSqlMeta.Injection.UPDATE_STATS_FIELD",
      injectionKey = "UPDATE_STATS_FIELD")
  private String updateField;

  @GuiWidgetElement(
      id = WIDGET_DELETE_FIELD,
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::ExecSqlDialog.DeleteField.Label",
      toolTip = "i18n::ExecSqlMeta.Injection.DELETE_STATS_FIELD",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "delete_field",
      injectionKeyDescription = "ExecSqlMeta.Injection.DELETE_STATS_FIELD",
      injectionKey = "DELETE_STATS_FIELD")
  private String deleteField;

  @GuiWidgetElement(
      id = WIDGET_READ_FIELD,
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::ExecSqlDialog.ReadField.Label",
      toolTip = "i18n::ExecSqlMeta.Injection.READ_STATS_FIELD",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_GENERAL,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "read_field",
      injectionKeyDescription = "ExecSqlMeta.Injection.READ_STATS_FIELD",
      injectionKey = "READ_STATS_FIELD")
  private String readField;

  @GuiWidgetElement(
      id = WIDGET_SQL_FROM_FILE,
      order = "0100",
      type = GuiElementType.FILENAME,
      typeFilename = TypeSqlFilename.class,
      label = "i18n::ExecSqlMeta.SqlFromFile.Label",
      toolTip = "i18n::ExecSqlMeta.SqlFromFile.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_SQL,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "sql_from_file",
      injectionKey = "SQL_FROM_FILE",
      injectionKeyDescription = "ExecSqlMeta.Injection.SQL_FROM_FILE")
  private String sqlFromFile;

  @HopMetadataProperty(
      injectionKeyDescription = "ExecSqlMeta.Injection.SQL",
      injectionKey = "SQL",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SQL)
  private String sql;

  @HopMetadataProperty(
      key = "execute_each_row",
      injectionKeyDescription = "ExecSqlMeta.Injection.EXECUTE_FOR_EACH_ROW",
      injectionKey = "EXECUTE_FOR_EACH_ROW")
  private boolean executedEachInputRow;

  @HopMetadataProperty(
      key = "single_statement",
      injectionKeyDescription = "ExecSqlMeta.Injection.EXECUTE_AS_SINGLE_STATEMENT",
      injectionKey = "EXECUTE_AS_SINGLE_STATEMENT")
  private boolean singleStatement;

  @HopMetadataProperty(
      key = "replace_variables",
      injectionKeyDescription = "ExecSqlMeta.Injection.REPLACE_VARIABLES",
      injectionKey = "REPLACE_VARIABLES")
  private boolean replaceVariables;

  @HopMetadataProperty(
      injectionKeyDescription = "ExecSqlMeta.Injection.QUOTE_STRINGS",
      injectionKey = "QUOTE_STRINGS")
  private boolean quoteString;

  @GuiWidgetElement(
      id = WIDGET_BIND_PARAMETERS,
      order = "0100",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ExecSqlDialog.SetParams.Label",
      toolTip = "i18n::ExecSqlDialog.SetParams.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_PARAMETERS,
      groupOrder = "30",
      groupType = GuiWidgetGroupType.TABS,
      getterMethod = "isParams",
      setterMethod = "setParams")
  @HopMetadataProperty(
      key = "set_params",
      injectionKeyDescription = "ExecSqlMeta.Injection.BIND_PARAMETERS",
      injectionKey = "BIND_PARAMETERS")
  private boolean params;

  @HopMetadataProperty(
      key = "argument",
      groupKey = "arguments",
      injectionGroupKey = "PARAMETERS",
      injectionGroupDescription = "ExecSqlMeta.Injection.PARAMETERS")
  private List<ExecSqlArgumentItem> arguments;

  public ExecSqlMeta() {
    super();
    arguments = new ArrayList<>();
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
            BaseMessages.getString(PKG, "ExecSqlMeta.Exception.CouldNotLoadSqlFromFile", path), e);
      }
    }
    return sql;
  }

  @Override
  public void setDefault() {
    sql = "";
    sqlFromFile = "";
    arguments = new ArrayList<>();
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
    RowMetaAndData add =
        ExecSql.getResultRow(
            new Result(), getUpdateField(), getInsertField(), getDeleteField(), getReadField());

    r.mergeRowMeta(add.getRowMeta());
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
                  PKG, "ExecSqlMeta.CheckResult.DatabaseMetaError", variables.resolve(connection)),
              transformMeta);
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.ConnectionExists"),
              transformMeta);
      remarks.add(cr);

      Database db = new Database(loggingObject, variables, databaseMeta);
      databases = new Database[] {db}; // keep track of it for
      // cancelling purposes...

      try {
        db.connect();
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.DBConnectionOK"),
                transformMeta);
        remarks.add(cr);

        String effectiveSql = null;
        try {
          effectiveSql = getEffectiveSql(variables);
        } catch (HopException e) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.CouldNotGetSql")
                      + e.getMessage(),
                  transformMeta);
          remarks.add(cr);
        }
        if (effectiveSql != null) {
          if (!Utils.isEmpty(effectiveSql)) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.SQLStatementEntered"),
                    transformMeta);
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.SQLStatementMissing"),
                    transformMeta);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.ErrorOccurred")
                    + e.getMessage(),
                transformMeta);
        remarks.add(cr);
      } finally {
        db.close();
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.ConnectionNeeded"),
              transformMeta);
      remarks.add(cr);
    }

    // If it's executed each row, make sure we have input
    if (executedEachInputRow) {
      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.TransformReceivingInfoOK"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.NoInputReceivedError"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.SQLOnlyExecutedOnce"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "ExecSqlMeta.CheckResult.InputReceivedOKForSQLOnlyExecuteOnce"),
                transformMeta);
        remarks.add(cr);
      }
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
      String impactSql;
      try {
        impactSql = getEffectiveSql(variables);
      } catch (HopException e) {
        impactSql = sql;
      }
      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ_WRITE,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              BaseMessages.getString(PKG, "ExecSqlMeta.DatabaseMeta.Unknown.Label"),
              BaseMessages.getString(PKG, "ExecSqlMeta.DatabaseMeta.Unknown2.Label"),
              BaseMessages.getString(PKG, "ExecSqlMeta.DatabaseMeta.Unknown3.Label"),
              transformMeta.getName(),
              impactSql,
              BaseMessages.getString(PKG, "ExecSqlMeta.DatabaseMeta.Title"));
      impact.add(ii);

    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection),
          e);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
