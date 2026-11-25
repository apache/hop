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

package org.apache.hop.pipeline.transforms.mergeintotable;

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
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.api.RelationalLineage;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Writes every input row to a table with one upsert statement: the row is inserted when its key is
 * new and updated when it exists, without a lookup query. The statement is a MERGE, an {@code
 * INSERT ... ON CONFLICT} or an {@code INSERT ... ON DUPLICATE KEY UPDATE}, whichever the dialect
 * declares in its {@link org.apache.hop.core.database.dml.DmlSyntax} (or the one forced by the
 * upsert form option).
 */
@Getter
@Setter
@GuiPlugin
@Transform(
    id = "MergeIntoTable",
    image = "mergeintotable.svg",
    name = "i18n::MergeIntoTable.Name",
    description = "i18n::MergeIntoTable.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::MergeIntoTableMeta.keyword",
    documentationUrl = "/pipeline/transforms/mergeintotable.html",
    actionTransformTypes = {ActionTransformType.OUTPUT, ActionTransformType.RDBMS})
@RelationalLineage(operation = RelationalIoOperation.WRITE)
public class MergeIntoTableMeta extends BaseTransformMeta<MergeIntoTable, MergeIntoTableData> {
  private static final Class<?> PKG = MergeIntoTableMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "MergeIntoTableDialog";

  public static final String WIDGET_CONNECTION = "connection";
  public static final String WIDGET_MERGE_SUPPORT = "mergeSupport";
  public static final String WIDGET_SCHEMA = "schemaName";
  public static final String WIDGET_TABLE = "tableName";
  public static final String WIDGET_BROWSE_TABLE = "browseTable";
  public static final String WIDGET_COMMIT_SIZE = "commitSize";
  public static final String WIDGET_USE_BATCH = "useBatchUpdate";
  public static final String WIDGET_UPSERT_FORM = "upsertForm";
  public static final String WIDGET_UPDATE_EXISTING = "updateExisting";
  public static final String WIDGET_UPDATE_ONLY_CHANGED = "updateOnlyChanged";
  public static final String WIDGET_KEY_FIELDS = "keyFields";
  public static final String WIDGET_VALUE_FIELDS = "valueFields";

  /** Column of the stream field in both field tables. 0 is the row number. */
  public static final int COLUMN_STREAM_FIELD = 1;

  /** Column of the table column in both field tables. */
  public static final int COLUMN_TABLE_COLUMN = 2;

  public static final String DEFAULT_COMMIT_SIZE = "1000";

  private static final String GROUP_GENERAL = "i18n::MergeIntoTableMeta.Group.General";
  private static final String GROUP_KEYS = "i18n::MergeIntoTableMeta.Group.Keys";
  private static final String GROUP_VALUES = "i18n::MergeIntoTableMeta.Group.Values";

  @GuiWidgetElement(
      id = WIDGET_CONNECTION,
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      label = "i18n::MergeIntoTableMeta.Connection.Label",
      toolTip = "i18n::MergeIntoTableMeta.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTION_NAME",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @GuiWidgetElement(
      id = WIDGET_SCHEMA,
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::MergeIntoTableMeta.Schema.Label",
      toolTip = "i18n::MergeIntoTableMeta.Schema.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "schema",
      injectionKey = "SCHEMA_NAME",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.Schema",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  @GuiWidgetElement(
      id = WIDGET_TABLE,
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::MergeIntoTableMeta.Table.Label",
      toolTip = "i18n::MergeIntoTableMeta.Table.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "table",
      injectionKey = "TABLE_NAME",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.Table",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  @GuiWidgetElement(
      id = WIDGET_COMMIT_SIZE,
      order = "0600",
      type = GuiElementType.TEXT,
      label = "i18n::MergeIntoTableMeta.CommitSize.Label",
      toolTip = "i18n::MergeIntoTableMeta.CommitSize.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "commit",
      injectionKey = "COMMIT_SIZE",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.CommitSize")
  private String commitSize;

  /**
   * Send the upsert statements in JDBC batches of the commit size, on a connection whose batch
   * failure contract is validated. A new transform has it on (see {@link #setDefault()}); a
   * transform loaded without the option writes row by row.
   */
  @GuiWidgetElement(
      id = WIDGET_USE_BATCH,
      order = "0700",
      type = GuiElementType.CHECKBOX,
      label = "i18n::MergeIntoTableMeta.UseBatch.Label",
      toolTip = "i18n::MergeIntoTableMeta.UseBatch.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "use_batch",
      injectionKey = "USE_BATCH",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.UseBatch")
  private boolean useBatchUpdate;

  /** The upsert statement to write; AUTOMATIC lets the dialect decide. */
  @GuiWidgetElement(
      id = WIDGET_UPSERT_FORM,
      order = "0250",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::MergeIntoTableMeta.UpsertForm.Label",
      toolTip = "i18n::MergeIntoTableMeta.UpsertForm.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "upsert_form",
      storeWithCode = true,
      injectionKey = "UPSERT_FORM",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.UpsertForm")
  private MergeUpsertForm upsertForm = MergeUpsertForm.AUTOMATIC;

  /** Update a row whose key exists. Off: insert only, existing rows are left alone. */
  @GuiWidgetElement(
      id = WIDGET_UPDATE_EXISTING,
      order = "0800",
      type = GuiElementType.CHECKBOX,
      label = "i18n::MergeIntoTableMeta.UpdateExisting.Label",
      toolTip = "i18n::MergeIntoTableMeta.UpdateExisting.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "update_existing",
      defaultBoolean = true,
      injectionKey = "UPDATE_EXISTING",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.UpdateExisting")
  private boolean updateExisting = true;

  /** Update a matched row only when one of its updated columns differs from the stream. */
  @GuiWidgetElement(
      id = WIDGET_UPDATE_ONLY_CHANGED,
      order = "0900",
      type = GuiElementType.CHECKBOX,
      label = "i18n::MergeIntoTableMeta.UpdateOnlyChanged.Label",
      toolTip = "i18n::MergeIntoTableMeta.UpdateOnlyChanged.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "update_only_changed",
      injectionKey = "UPDATE_ONLY_CHANGED",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.UpdateOnlyChanged")
  private boolean updateOnlyChanged;

  @GuiWidgetElement(
      id = WIDGET_KEY_FIELDS,
      order = "0100",
      type = GuiElementType.TABLE,
      label = "i18n::MergeIntoTableMeta.KeyFields.Label",
      toolTip = "i18n::MergeIntoTableMeta.KeyFields.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KEYS,
      groupOrder = "0200",
      tableRows = 8)
  @HopMetadataProperty(
      groupKey = "keys",
      key = "key",
      injectionGroupKey = "KEYS",
      injectionGroupDescription = "MergeIntoTableMeta.Injection.Keys",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.Key")
  private List<MergeKeyField> keyFields = new ArrayList<>();

  @GuiWidgetElement(
      id = WIDGET_VALUE_FIELDS,
      order = "0100",
      type = GuiElementType.TABLE,
      label = "i18n::MergeIntoTableMeta.ValueFields.Label",
      toolTip = "i18n::MergeIntoTableMeta.ValueFields.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_VALUES,
      groupOrder = "0300",
      tableRows = 12)
  @HopMetadataProperty(
      groupKey = "values",
      key = "value",
      injectionGroupKey = "VALUES",
      injectionGroupDescription = "MergeIntoTableMeta.Injection.Values",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.Value")
  private List<MergeValueField> valueFields = new ArrayList<>();

  public MergeIntoTableMeta() {
    super();
  }

  @Override
  public MergeIntoTableMeta clone() {
    MergeIntoTableMeta clone = (MergeIntoTableMeta) super.clone();
    clone.keyFields = new ArrayList<>();
    for (MergeKeyField keyField : keyFields) {
      clone.keyFields.add(new MergeKeyField(keyField));
    }
    clone.valueFields = new ArrayList<>();
    for (MergeValueField valueField : valueFields) {
      clone.valueFields.add(new MergeValueField(valueField));
    }
    return clone;
  }

  @Override
  public void setDefault() {
    commitSize = DEFAULT_COMMIT_SIZE;
    // A failed batch is recovered where the connection's contract is validated, so batching is
    // safe with an error hop; elsewhere the rows run one statement each.
    useBatchUpdate = true;
    upsertForm = MergeUpsertForm.AUTOMATIC;
    updateExisting = true;
    updateOnlyChanged = false;
  }

  /**
   * The Target tab shows a Browse button. The dialog opens the database explorer itself, so it can
   * use its own shell and write the schema and table widgets.
   */
  @GuiWidgetElement(
      id = WIDGET_BROWSE_TABLE,
      order = "0500",
      type = GuiElementType.BUTTON,
      label = "i18n::MergeIntoTableMeta.BrowseTable.Label",
      toolTip = "i18n::MergeIntoTableMeta.BrowseTable.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  public void browseTable(Object sourceObject) {
    // The dialog's composite buttons listener handles the click.
  }

  /**
   * A line under the connection that stays empty while the connection can run a MERGE, and says why
   * not when it can't. The dialog sets its text; there is nothing to click.
   */
  @GuiWidgetElement(
      id = WIDGET_MERGE_SUPPORT,
      order = "0200",
      type = GuiElementType.LINK,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GENERAL,
      groupOrder = "0100")
  public void mergeSupport() {
    // Information only.
  }

  /**
   * @param variables resolves a variable in the commit size
   * @return the commit size; 0 when it is blank or not a number
   */
  public int getCommitSize(IVariables variables) {
    String resolved = variables.resolve(Const.NVL(commitSize, ""));
    if (Utils.isEmpty(resolved)) {
      return 0;
    }
    String expanded = Const.expandIntegerString(resolved.trim());
    return Const.toInt(expanded != null ? expanded : resolved.trim(), 0);
  }

  /**
   * @return the upsert form asked for, or null to let the dialect decide
   */
  public UpsertForm getRequestedUpsertForm() {
    return MergeUpsertForm.upsertFormOf(upsertForm);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  private DatabaseMeta findDatabaseMeta(
      PipelineMeta pipelineMeta, IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    DatabaseMeta databaseMeta = null;
    if (pipelineMeta != null) {
      databaseMeta = pipelineMeta.findDatabase(connection, variables);
    }
    if (databaseMeta == null && metadataProvider != null && !Utils.isEmpty(connection)) {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    }
    return databaseMeta;
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

    if (input != null && input.length > 0) {
      ok(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.ReceivingInput");
    } else {
      error(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.NoInput");
    }

    if (Utils.isEmpty(variables.resolve(tableName))) {
      error(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.NoTable");
    }
    if (keyFields == null || keyFields.isEmpty()) {
      error(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.NoKeys");
    }
    checkStreamFields(remarks, transformMeta, prev, variables);

    DatabaseMeta databaseMeta;
    try {
      databaseMeta = findDatabaseMeta(pipelineMeta, variables, metadataProvider);
    } catch (Exception e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "MergeIntoTableMeta.CheckResult.ConnectionNotFound", connection),
              transformMeta));
      return;
    }
    if (databaseMeta == null && !Utils.isEmpty(connection)) {
      error(
          remarks, transformMeta, "MergeIntoTableMeta.CheckResult.ConnectionNotFound", connection);
      return;
    }

    // The dialect first: an unsupported database is reported without connecting to it.
    String reason = DmlSupport.unsupportedReason(databaseMeta, getRequestedUpsertForm());
    if (reason != null) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, reason, transformMeta));
      return;
    }
    ok(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.MergeSupported");

    if (Utils.isEmpty(variables.resolve(tableName))) {
      return;
    }
    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      db.connect();
      reason =
          DmlSupport.unsupportedReason(
              databaseMeta, db.getDatabaseMetaData(), getRequestedUpsertForm());
      if (reason != null) {
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, reason, transformMeta));
        return;
      }
      String schema = variables.resolve(schemaName);
      String table = variables.resolve(tableName);
      IRowMeta tableFields = db.getTableFieldsMeta(schema, table);
      if (tableFields == null) {
        error(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.TableNotFound", table);
        return;
      }
      // Resolved as the transform resolves them when it runs.
      List<String> missing = new ArrayList<>();
      for (MergeKeyField keyField : keyFields) {
        String column = variables.resolve(keyField.getTableColumn());
        if (tableFields.searchValueMeta(column) == null) {
          missing.add(column);
        }
      }
      for (MergeValueField valueField : valueFields) {
        String column = variables.resolve(valueField.getTableColumn());
        if (tableFields.searchValueMeta(column) == null) {
          missing.add(column);
        }
      }
      if (missing.isEmpty()) {
        ok(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.AllColumnsFound");
      } else {
        error(
            remarks,
            transformMeta,
            "MergeIntoTableMeta.CheckResult.MissingColumns",
            String.join(", ", missing));
      }
    } catch (HopException e) {
      error(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.DatabaseError", e.getMessage());
    }
  }

  private void checkStreamFields(
      List<ICheckResult> remarks,
      TransformMeta transformMeta,
      IRowMeta prev,
      IVariables variables) {
    if (prev == null || prev.isEmpty()) {
      return;
    }
    List<String> missing = new ArrayList<>();
    for (MergeKeyField keyField : keyFields) {
      String field = variables.resolve(keyField.getStreamField());
      if (prev.searchValueMeta(field) == null) {
        missing.add(field);
      }
    }
    for (MergeValueField valueField : valueFields) {
      String field = variables.resolve(valueField.getStreamField());
      if (prev.searchValueMeta(field) == null) {
        missing.add(field);
      }
    }
    if (missing.isEmpty()) {
      ok(remarks, transformMeta, "MergeIntoTableMeta.CheckResult.AllStreamFieldsFound");
    } else {
      error(
          remarks,
          transformMeta,
          "MergeIntoTableMeta.CheckResult.MissingStreamFields",
          String.join(", ", missing));
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
      IHopMetadataProvider metadataProvider) {
    if (prev == null) {
      return;
    }
    String databaseName = "";
    try {
      DatabaseMeta databaseMeta = findDatabaseMeta(pipelineMeta, variables, metadataProvider);
      if (databaseMeta != null) {
        databaseName = databaseMeta.getDatabaseName();
      }
    } catch (Exception e) {
      // The impact is still listed, without the database name.
    }
    String table = variables.resolve(tableName);
    for (MergeKeyField keyField : keyFields) {
      impact.add(
          impact(
              DatabaseImpact.TYPE_IMPACT_READ_WRITE,
              pipelineMeta,
              transformMeta,
              databaseName,
              table,
              keyField.getTableColumn(),
              keyField.getStreamField(),
              prev));
    }
    for (MergeValueField valueField : valueFields) {
      impact.add(
          impact(
              DatabaseImpact.TYPE_IMPACT_WRITE,
              pipelineMeta,
              transformMeta,
              databaseName,
              table,
              valueField.getTableColumn(),
              valueField.getStreamField(),
              prev));
    }
  }

  private static DatabaseImpact impact(
      int type,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      String databaseName,
      String table,
      String column,
      String streamField,
      IRowMeta prev) {
    IValueMeta valueMeta = prev.searchValueMeta(streamField);
    return new DatabaseImpact(
        type,
        pipelineMeta.getName(),
        transformMeta.getName(),
        databaseName,
        table,
        column,
        streamField,
        valueMeta != null ? valueMeta.getOrigin() : "?",
        "",
        valueMeta != null ? "Type = " + valueMeta.toStringMeta() : "");
  }

  private static void ok(List<ICheckResult> remarks, TransformMeta transformMeta, String key) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, key), transformMeta));
  }

  private static void error(
      List<ICheckResult> remarks, TransformMeta transformMeta, String key, String... arguments) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString(PKG, key, (Object[]) arguments),
            transformMeta));
  }
}
