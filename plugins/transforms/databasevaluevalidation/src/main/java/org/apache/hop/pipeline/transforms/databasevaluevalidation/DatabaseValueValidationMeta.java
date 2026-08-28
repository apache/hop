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
package org.apache.hop.pipeline.transforms.databasevaluevalidation;

import java.util.ArrayList;
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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;

@Getter
@Setter
@Transform(
    id = "DatabaseValueValidation",
    image = "databasevaluevalidation.svg",
    name = "i18n::DatabaseValueValidation.Name",
    description = "i18n::DatabaseValueValidation.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Validation",
    keywords = "i18n::DatabaseValueValidationMeta.keyword",
    documentationUrl = "/pipeline/transforms/databasevaluevalidation.html",
    actionTransformTypes = {ActionTransformType.RDBMS})
@GuiPlugin
public class DatabaseValueValidationMeta
    extends BaseTransformMeta<DatabaseValueValidation, DatabaseValueValidationData> {
  private static final Class<?> PKG = DatabaseValueValidationMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID =
      "DATABASE_VALUE_VALIDATION_DIALOG_OPTIONS";
  public static final String GROUP_TARGET = "Target";
  public static final String GROUP_OPTIONS = "Options";

  @GuiWidgetElement(
      id = "connectionName",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      label = "i18n::DatabaseValueValidationMeta.ConnectionName.Label",
      toolTip = "i18n::DatabaseValueValidationMeta.ConnectionName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_TARGET,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION,
      storeWithCode = true)
  private String connectionName;

  @GuiWidgetElement(
      id = "schemaName",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::DatabaseValueValidationMeta.SchemaName.Label",
      toolTip = "i18n::DatabaseValueValidationMeta.SchemaName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_TARGET,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "schema",
      injectionKey = "TARGET_SCHEMA",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.SchemaName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  @GuiWidgetElement(
      id = "tableName",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::DatabaseValueValidationMeta.TableName.Label",
      toolTip = "i18n::DatabaseValueValidationMeta.TableName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_TARGET,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "table",
      injectionKey = "TARGET_TABLE",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.TableName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  @GuiWidgetElement(
      id = "browseTable",
      order = "0400",
      type = GuiElementType.BUTTON,
      label = "i18n::DatabaseValueValidationMeta.BrowseTable.Label",
      toolTip = "i18n::DatabaseValueValidationMeta.BrowseTable.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_TARGET,
      groupOrder = "0100")
  public void browseTable(Object object) {
    if (!(object instanceof DatabaseValueValidationMeta meta)) {
      return;
    }
    HopGui hopGui = HopGui.getInstance();
    if (hopGui == null) {
      return;
    }
    try {
      String name = hopGui.getVariables().resolve(Const.NVL(meta.getConnectionName(), ""));
      if (Utils.isEmpty(name)) {
        MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_ERROR);
        box.setMessage(
            BaseMessages.getString(PKG, "DatabaseValueValidationDialog.ConnectionMissing.Message"));
        box.setText(
            BaseMessages.getString(PKG, "DatabaseValueValidationDialog.ConnectionMissing.Title"));
        box.open();
        return;
      }
      DatabaseMeta databaseMeta =
          hopGui.getMetadataProvider().getSerializer(DatabaseMeta.class).load(name);
      if (databaseMeta == null) {
        MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_ERROR);
        box.setMessage(
            BaseMessages.getString(PKG, "DatabaseValueValidationDialog.ConnectionMissing.Message"));
        box.setText(
            BaseMessages.getString(PKG, "DatabaseValueValidationDialog.ConnectionMissing.Title"));
        box.open();
        return;
      }
      DatabaseExplorerDialog explorer =
          new DatabaseExplorerDialog(
              hopGui.getShell(),
              SWT.NONE,
              hopGui.getVariables(),
              databaseMeta,
              List.of(databaseMeta));
      explorer.setSelectedSchemaAndTable(meta.getSchemaName(), meta.getTableName());
      if (explorer.open()) {
        meta.setSchemaName(Const.NVL(explorer.getSchemaName(), ""));
        meta.setTableName(Const.NVL(explorer.getTableName(), ""));
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "DatabaseValueValidationDialog.BrowseError.Title"),
          BaseMessages.getString(PKG, "DatabaseValueValidationDialog.BrowseError.Message"),
          e);
    }
  }

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionKey = "DATABASE_FIELD",
      injectionGroupKey = "DATABASE_FIELDS",
      injectionGroupDescription = "DatabaseValueValidationMeta.Injection.Fields",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.Field",
      hopMetadataPropertyType = HopMetadataPropertyType.FIELD_LIST)
  private List<DatabaseValueValidationField> fields;

  @GuiWidgetElement(
      id = "omitValues",
      order = "0100",
      type = GuiElementType.CHECKBOX,
      label = "i18n::DatabaseValueValidationMeta.OmitValues.Label",
      toolTip = "i18n::DatabaseValueValidationMeta.OmitValues.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = "0300")
  @HopMetadataProperty(
      key = "omit_values",
      injectionKey = "OMIT_VALUES",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.OmitValues")
  private boolean omitValues;

  @GuiWidgetElement(
      id = "failIfRequiredColumnsUnmapped",
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "i18n::DatabaseValueValidationMeta.FailUnmapped.Label",
      toolTip = "i18n::DatabaseValueValidationMeta.FailUnmapped.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = "0300")
  @HopMetadataProperty(
      key = "fail_unmapped_required",
      injectionKey = "FAIL_UNMAPPED_REQUIRED",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.FailUnmapped")
  private boolean failIfRequiredColumnsUnmapped;

  @GuiWidgetElement(
      id = "concatenationSeparator",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::DatabaseValueValidationMeta.Separator.Label",
      toolTip = "i18n::DatabaseValueValidationMeta.Separator.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = "0300")
  @HopMetadataProperty(
      key = "concat_separator",
      injectionKey = "CONCATENATION_SEPARATOR",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.Separator")
  private String concatenationSeparator;

  public DatabaseValueValidationMeta() {
    fields = new ArrayList<>();
    failIfRequiredColumnsUnmapped = true;
    concatenationSeparator = "; ";
  }

  @Override
  public void setDefault() {
    failIfRequiredColumnsUnmapped = true;
    concatenationSeparator = "; ";
    omitValues = false;
  }

  @Override
  public Object clone() {
    DatabaseValueValidationMeta clone = (DatabaseValueValidationMeta) super.clone();
    clone.fields = new ArrayList<>();
    if (fields != null) {
      for (DatabaseValueValidationField field : fields) {
        clone.fields.add(new DatabaseValueValidationField(field));
      }
    }
    return clone;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public IRowMeta loadTableFields(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    if (Utils.isEmpty(connectionName)) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "DatabaseValueValidationMeta.Exception.ConnectionNotDefined"));
    }
    if (Utils.isEmpty(variables.resolve(Const.NVL(tableName, "")))) {
      throw new HopException(
          BaseMessages.getString(PKG, "DatabaseValueValidationMeta.Exception.TableNotSpecified"));
    }
    DatabaseMeta databaseMeta =
        metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connectionName));
    if (databaseMeta == null) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "DatabaseValueValidationMeta.Exception.ConnectionNotDefined"));
    }
    String schema = variables.resolve(Const.NVL(schemaName, ""));
    String table = variables.resolve(tableName);
    try (Database db =
        new Database(new LoggingObject("DatabaseValueValidation"), variables, databaseMeta)) {
      db.connect();
      if (!db.checkTableExists(schema, table)) {
        throw new HopException(
            BaseMessages.getString(PKG, "DatabaseValueValidationMeta.Exception.TableNotFound"));
      }
      return db.getTableFieldsMeta(schema, table);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "DatabaseValueValidationMeta.Exception.ErrorGettingFields"),
          e);
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
    if (Utils.isEmpty(connectionName)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DatabaseValueValidationMeta.CheckResult.NoConnection"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DatabaseValueValidationMeta.CheckResult.ConnectionOk"),
              transformMeta));
    }
    if (Utils.isEmpty(tableName)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DatabaseValueValidationMeta.CheckResult.NoTable"),
              transformMeta));
    }
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DatabaseValueValidationMeta.CheckResult.ReceivingInfo"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DatabaseValueValidationMeta.CheckResult.NoInput"),
              transformMeta));
    }
  }
}
