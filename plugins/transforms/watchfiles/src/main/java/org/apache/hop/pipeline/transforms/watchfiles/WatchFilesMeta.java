/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@GuiPlugin
@Transform(
    id = "WatchFiles",
    name = "i18n::WatchFilesMeta.Name",
    description = "i18n::WatchFilesMeta.Description",
    image = "watchfiles.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::WatchFilesMeta.Keywords",
    supportedEngines = {"Local"},
    documentationUrl = "/pipeline/transforms/watchfiles.html")
public class WatchFilesMeta extends BaseTransformMeta<WatchFiles, WatchFilesData> {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "WATCH_FILES_OPTIONS";
  public static final String REPLAY_GUI_PARENT_ID = "WATCH_FILES_REPROCESS";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "directory",
      order = "0100",
      type = GuiElementType.FOLDER,
      label = "i18n::WatchFilesMeta.directory.Label",
      toolTip = "i18n::WatchFilesMeta.directory.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private String directory;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "editWatchId",
      order = "0150",
      type = GuiElementType.CHECKBOX,
      label = "i18n::WatchFilesMeta.editWatchId.Label",
      toolTip = "i18n::WatchFilesMeta.editWatchId.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private boolean editWatchId;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "watchId",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.watchId.Label",
      toolTip = "i18n::WatchFilesMeta.watchId.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String watchId;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "patternSyntax",
      order = "0250",
      type = GuiElementType.COMBO,
      label = "i18n::WatchFilesMeta.patternSyntax.Label",
      toolTip = "i18n::WatchFilesMeta.patternSyntax.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01",
      comboValuesMethod = "patternSyntaxValues",
      variables = false)
  private String patternSyntax = "REGEXP";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "includeWildcard",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.includeWildcard.Label",
      toolTip = "i18n::WatchFilesMeta.includeWildcard.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private String includeWildcard;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "excludeWildcard",
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.excludeWildcard.Label",
      toolTip = "i18n::WatchFilesMeta.excludeWildcard.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private String excludeWildcard;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "includeSubdirectories",
      order = "0500",
      type = GuiElementType.CHECKBOX,
      label = "i18n::WatchFilesMeta.includeSubdirectories.Label",
      toolTip = "i18n::WatchFilesMeta.includeSubdirectories.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private boolean includeSubdirectories = false;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "strategy",
      order = "0600",
      type = GuiElementType.COMBO,
      label = "i18n::WatchFilesMeta.strategy.Label",
      toolTip = "i18n::WatchFilesMeta.strategy.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02",
      comboValuesMethod = "strategyValues",
      variables = false)
  private String strategy = "AUTO";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "created",
      order = "0700",
      type = GuiElementType.CHECKBOX,
      label = "i18n::WatchFilesMeta.created.Label",
      toolTip = "i18n::WatchFilesMeta.created.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private boolean created = true;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "modified",
      order = "0800",
      type = GuiElementType.CHECKBOX,
      label = "i18n::WatchFilesMeta.modified.Label",
      toolTip = "i18n::WatchFilesMeta.modified.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private boolean modified = true;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "deleted",
      order = "0900",
      type = GuiElementType.CHECKBOX,
      label = "i18n::WatchFilesMeta.deleted.Label",
      toolTip = "i18n::WatchFilesMeta.deleted.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private boolean deleted = false;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "pollingInterval",
      order = "1000",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.pollingInterval.Label",
      toolTip = "i18n::WatchFilesMeta.pollingInterval.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String pollingInterval = "5000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "reconciliationInterval",
      order = "1100",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.reconciliationInterval.Label",
      toolTip = "i18n::WatchFilesMeta.reconciliationInterval.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String reconciliationInterval = "60000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "eventCapacity",
      order = "1200",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.eventCapacity.Label",
      toolTip = "i18n::WatchFilesMeta.eventCapacity.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String eventCapacity = "4096";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "maximumEntries",
      order = "1300",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.maximumEntries.Label",
      toolTip = "i18n::WatchFilesMeta.maximumEntries.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String maximumEntries = "100000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "waitUntilStable",
      order = "1400",
      type = GuiElementType.CHECKBOX,
      label = "i18n::WatchFilesMeta.waitUntilStable.Label",
      toolTip = "i18n::WatchFilesMeta.waitUntilStable.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private boolean waitUntilStable = true;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "minimumAge",
      order = "1500",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.minimumAge.Label",
      toolTip = "i18n::WatchFilesMeta.minimumAge.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String minimumAge = "2000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "stabilityChecks",
      order = "1600",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.stabilityChecks.Label",
      toolTip = "i18n::WatchFilesMeta.stabilityChecks.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String stabilityChecks = "2";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "stabilityInterval",
      order = "1700",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.stabilityInterval.Label",
      toolTip = "i18n::WatchFilesMeta.stabilityInterval.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String stabilityInterval = "1000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "stateDirectory",
      order = "1800",
      type = GuiElementType.FOLDER,
      label = "i18n::WatchFilesMeta.stateDirectory.Label",
      toolTip = "i18n::WatchFilesMeta.stateDirectory.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String stateDirectory = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "initialScan",
      order = "1900",
      type = GuiElementType.COMBO,
      label = "i18n::WatchFilesMeta.initialScan.Label",
      toolTip = "i18n::WatchFilesMeta.initialScan.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01",
      comboValuesMethod = "initialScanValues",
      variables = false)
  private String initialScan = "COMPARE_WITH_STATE";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "checkpointInterval",
      order = "2000",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.checkpointInterval.Label",
      toolTip = "i18n::WatchFilesMeta.checkpointInterval.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String checkpointInterval = "5000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "diagnosticsInterval",
      order = "2100",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.diagnosticsInterval.Label",
      toolTip = "i18n::WatchFilesMeta.diagnosticsInterval.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String diagnosticsInterval = "60000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "slowOperationThreshold",
      order = "2200",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.slowOperationThreshold.Label",
      toolTip = "i18n::WatchFilesMeta.slowOperationThreshold.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.Advanced",
      groupOrder = "02")
  private String slowOperationThreshold = "30000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "replayFilter",
      order = "2300",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.replayFilter.Label",
      toolTip = "i18n::WatchFilesMeta.replayFilter.Tooltip",
      parentId = REPLAY_GUI_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::WatchFilesMeta.Group.Reprocess",
      groupOrder = "01")
  private String replayFilter = ".*";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "maximumRunTime",
      order = "1950",
      type = GuiElementType.TEXT,
      label = "i18n::WatchFilesMeta.maximumRunTime.Label",
      toolTip = "i18n::WatchFilesMeta.maximumRunTime.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01")
  private String maximumRunTime = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "maximumRunTimeUnit",
      order = "1960",
      type = GuiElementType.COMBO,
      label = "i18n::WatchFilesMeta.maximumRunTimeUnit.Label",
      toolTip = "i18n::WatchFilesMeta.maximumRunTimeUnit.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::WatchFilesMeta.Group.General",
      groupOrder = "01",
      comboValuesMethod = "maximumRunTimeUnitValues",
      variables = false)
  private String maximumRunTimeUnit = "MINUTES";

  public List<String> maximumRunTimeUnitValues(
      org.apache.hop.core.logging.ILogChannel log, IHopMetadataProvider provider) {
    return guiValues("maximumRunTimeUnit");
  }

  public long maximumRunMillis(IVariables variables) {
    String value = variables.resolve(maximumRunTime);
    if (value == null || value.isBlank()) return 0;
    try {
      long multiplier =
          switch (maximumRunTimeUnit) {
            case "MINUTES" -> 60000L;
            case "HOURS" -> 3600000L;
            default -> throw new IllegalArgumentException("Unknown runtime unit.");
          };
      long millis =
          new java.math.BigDecimal(value.trim())
              .multiply(java.math.BigDecimal.valueOf(multiplier))
              .longValueExact();
      if (millis <= 0) throw new IllegalArgumentException("Non-positive runtime.");
      return millis;
    } catch (RuntimeException failure) {
      throw new IllegalArgumentException(
          "Maximum run time must be blank or a positive number in minutes or hours, representable in whole milliseconds.",
          failure);
    }
  }

  public List<String> strategyValues(
      org.apache.hop.core.logging.ILogChannel log, IHopMetadataProvider provider) {
    return guiValues("strategy");
  }

  public List<String> patternSyntaxValues(
      org.apache.hop.core.logging.ILogChannel log, IHopMetadataProvider provider) {
    return guiValues("patternSyntax");
  }

  public String filenameRegex(IVariables variables, String value) {
    return FilenamePatternSyntax.valueOf(patternSyntax).toRegex(variables.resolve(value));
  }

  public List<String> initialScanValues(
      org.apache.hop.core.logging.ILogChannel log, IHopMetadataProvider provider) {
    return guiValues("initialScan");
  }

  private static List<String> optionValues(String setting) {
    return switch (setting) {
      case "strategy" -> List.of("AUTO", "NATIVE", "POLLING");
      case "maximumRunTimeUnit" -> List.of("MINUTES", "HOURS");
      case "patternSyntax" -> List.of("WILDCARD", "REGEXP");
      case "initialScan" -> List.of("EMIT_EXISTING", "IGNORE_EXISTING", "COMPARE_WITH_STATE");
      default -> throw new IllegalArgumentException(setting);
    };
  }

  private static List<String> guiValues(String setting) {
    return optionValues(setting).stream()
        .map(value -> optionLabel(setting, value))
        .distinct()
        .toList();
  }

  static String optionLabel(String setting, String value) {
    if (value == null || !optionValues(setting).contains(value)) return value;
    if ("initialScan".equals(setting) && "COMPARE_WITH_STATE".equals(value)) {
      value = "IGNORE_EXISTING";
    }
    return BaseMessages.getString(WatchFilesMeta.class, "WatchFilesMeta." + setting + "." + value);
  }

  static String optionValue(String setting, String label) {
    return optionValues(setting).stream()
        .filter(value -> value.equals(label) || optionLabel(setting, value).equals(label))
        .findFirst()
        .orElse(label);
  }

  @Override
  public boolean consumesMainInput() {
    return false;
  }

  @Override
  public boolean canStartWithoutInput() {
    return true;
  }

  @Override
  public PipelineMeta.PipelineType[] getSupportedPipelineTypes() {
    return new PipelineMeta.PipelineType[] {PipelineMeta.PipelineType.Normal};
  }

  @Override
  public void setDefault() {
    directory = "";
    if (watchId == null || watchId.isBlank()) {
      watchId = "watch-" + UUID.randomUUID();
    }
    editWatchId = false;
    patternSyntax = "WILDCARD";
    includeWildcard = "";
    excludeWildcard = "";
    includeSubdirectories = false;
    strategy = "AUTO";
    created = true;
    modified = true;
    deleted = false;
    pollingInterval = "5000";
    reconciliationInterval = "60000";
    eventCapacity = "4096";
    maximumEntries = "100000";
    waitUntilStable = true;
    minimumAge = "2000";
    stabilityChecks = "2";
    stabilityInterval = "1000";
    stateDirectory =
        java.nio.file.Path.of(System.getProperty("user.home"), ".hop", "watch-files").toString();
    initialScan = "COMPARE_WITH_STATE";
    checkpointInterval = "5000";
    diagnosticsInterval = "60000";
    slowOperationThreshold = "30000";
    replayFilter = ".*";
    maximumRunTime = "";
    maximumRunTimeUnit = "MINUTES";
  }

  public void validate(IVariables variables) {
    if (variables.resolve(directory) == null || variables.resolve(directory).isBlank()) {
      throw new IllegalArgumentException("Directory is required.");
    }
    String id = variables.resolve(watchId);
    if (id == null || !id.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) {
      throw new IllegalArgumentException(
          "Watch ID must contain 1-128 letters, digits, underscores or hyphens.");
    }
    if (variables.resolve(stateDirectory) == null || variables.resolve(stateDirectory).isBlank()) {
      throw new IllegalArgumentException("A local state directory is required.");
    }
    DetectionStrategy.valueOf(strategy);
    InitialScan.valueOf(initialScan);
    maximumRunMillis(variables);
    java.util.regex.Pattern.compile(filenameRegex(variables, includeWildcard));
    java.util.regex.Pattern.compile(filenameRegex(variables, excludeWildcard));
    number(variables, pollingInterval, 1, "Polling interval");
    number(variables, reconciliationInterval, 1, "Reconciliation interval");
    number(variables, checkpointInterval, 1, "Checkpoint interval");
    number(variables, diagnosticsInterval, 1, "Diagnostics interval");
    number(variables, slowOperationThreshold, 1, "Slow operation threshold");
    java.util.regex.Pattern.compile(variables.resolve(replayFilter));
    number(variables, minimumAge, 0, "Minimum age");
    number(variables, stabilityInterval, 1, "Stability interval");
    integer(variables, stabilityChecks, "Stability checks");
    integer(variables, eventCapacity, "Native hint capacity");
    integer(variables, maximumEntries, "Maximum entries");
    if (!created && !modified && !deleted) {
      throw new IllegalArgumentException("Enable at least one event type.");
    }
  }

  public static long number(IVariables variables, String value, long minimum, String label) {
    try {
      long parsed = Long.parseLong(variables.resolve(value));
      if (parsed < minimum || parsed > Integer.MAX_VALUE) {
        throw new NumberFormatException();
      }
      return parsed;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          label + " must be between " + minimum + " and " + Integer.MAX_VALUE + ".", e);
    }
  }

  public static int integer(IVariables variables, String value, String label) {
    return (int) number(variables, value, 1, label);
  }

  public boolean enabled(FileEventType type) {
    return switch (type) {
      case CREATED -> created;
      case MODIFIED -> modified;
      case DELETED -> deleted;
    };
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
    try {
      validate(variables);
      if (input.length != 0) {
        throw new IllegalArgumentException(
            "Watch Files is a source and does not accept input rows.");
      }
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK, "Watch Files configuration is valid.", transformMeta));
    } catch (RuntimeException e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }
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
    row.clear();
    IValueMeta[] fields = {
      new ValueMetaString("filename"),
      new ValueMetaString("short_filename"),
      new ValueMetaString("path"),
      new ValueMetaString("uri"),
      new ValueMetaString("event_type"),
      new ValueMetaInteger("size"),
      new ValueMetaDate("last_modified"),
      new ValueMetaDate("detected_at"),
      new ValueMetaBoolean("is_directory"),
      new ValueMetaString("scheme"),
      new ValueMetaInteger("previous_size"),
      new ValueMetaDate("previous_last_modified"),
      new ValueMetaString("watch_id")
    };
    for (IValueMeta field : fields) {
      field.setOrigin(name);
      row.addValueMeta(field);
    }
  }
}
