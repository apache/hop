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
package org.apache.hop.lint;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/**
 * Lint a project or a folder, and succeed or fail on a severity threshold.
 *
 * <p>The same engine, rule packs and hop-lint.yml as {@code hop lint}, through {@link LintRun}, so
 * the two give the same findings and the same outcome on the same project. Headless: it runs on Hop
 * Server and in containers.
 */
@Action(
    id = "RUN_LINTER",
    name = "i18n::ActionRunLinter.Name",
    description = "i18n::ActionRunLinter.Description",
    image = "lint-check.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionRunLinter.Keywords",
    documentationUrl = "/workflow/actions/runlinter.html")
@GuiPlugin
@Getter
@Setter
public class ActionRunLinter extends ActionBase implements IAction {
  private static final Class<?> PKG = ActionRunLinter.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "RUN_LINTER_ACTION_DIALOG_OPTIONS";

  public static final String GROUP_TARGET = "i18n::ActionRunLinter.Group.Target";
  public static final String GROUP_OUTCOME = "i18n::ActionRunLinter.Group.Outcome";
  public static final String GROUP_REPORT = "i18n::ActionRunLinter.Group.Report";

  public static final String VAR_ERRORS = "LINT_ERRORS";
  public static final String VAR_WARNINGS = "LINT_WARNINGS";
  public static final String VAR_INFOS = "LINT_INFOS";
  public static final String VAR_REPORT_FILE = "LINT_REPORT_FILE";

  /** The folder to lint; empty lints the current project. */
  @GuiWidgetElement(
      id = "RUN_LINTER_TARGET",
      order = "0100",
      type = GuiElementType.FOLDER,
      label = "i18n::ActionRunLinter.Target.Label",
      toolTip = "i18n::ActionRunLinter.Target.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_TARGET,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "target")
  private String target;

  @GuiWidgetElement(
      id = "RUN_LINTER_INCLUDE_METADATA",
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ActionRunLinter.IncludeMetadata.Label",
      toolTip = "i18n::ActionRunLinter.IncludeMetadata.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_TARGET,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "include_metadata")
  private boolean includeMetadata;

  /** A hop-lint.yml to use instead of the project's; empty uses the project's. */
  @GuiWidgetElement(
      id = "RUN_LINTER_CONFIG_FILE",
      order = "0300",
      type = GuiElementType.FILENAME,
      typeFilename = LintConfigTypeFilename.class,
      label = "i18n::ActionRunLinter.ConfigFile.Label",
      toolTip = "i18n::ActionRunLinter.ConfigFile.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_TARGET,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "config_file")
  private String configFile;

  @GuiWidgetElement(
      id = "RUN_LINTER_MINIMUM_SEVERITY",
      order = "0400",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::ActionRunLinter.MinimumSeverity.Label",
      toolTip = "i18n::ActionRunLinter.MinimumSeverity.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OUTCOME,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "minimum_severity")
  private LintSeverity.Level minimumSeverity;

  @GuiWidgetElement(
      id = "RUN_LINTER_FAIL_ON",
      order = "0500",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::ActionRunLinter.FailOn.Label",
      toolTip = "i18n::ActionRunLinter.FailOn.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OUTCOME,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "fail_on")
  private LintSeverity.FailOn failOn;

  /** Fail when more warnings than this are found; empty for no limit. */
  @GuiWidgetElement(
      id = "RUN_LINTER_MAX_WARNINGS",
      order = "0600",
      type = GuiElementType.TEXT,
      label = "i18n::ActionRunLinter.MaxWarnings.Label",
      toolTip = "i18n::ActionRunLinter.MaxWarnings.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OUTCOME,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "max_warnings")
  private String maxWarnings;

  @GuiWidgetElement(
      id = "RUN_LINTER_BASELINE_FILE",
      order = "0700",
      type = GuiElementType.FILENAME,
      typeFilename = LintBaselineTypeFilename.class,
      label = "i18n::ActionRunLinter.BaselineFile.Label",
      toolTip = "i18n::ActionRunLinter.BaselineFile.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OUTCOME,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "baseline_file")
  private String baselineFile;

  @GuiWidgetElement(
      id = "RUN_LINTER_REPORT_FILE",
      order = "0800",
      type = GuiElementType.FILENAME,
      label = "i18n::ActionRunLinter.ReportFile.Label",
      toolTip = "i18n::ActionRunLinter.ReportFile.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_REPORT,
      groupOrder = "30",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "report_file")
  private String reportFile;

  @GuiWidgetElement(
      id = "RUN_LINTER_REPORT_FORMAT",
      order = "0900",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::ActionRunLinter.ReportFormat.Label",
      toolTip = "i18n::ActionRunLinter.ReportFormat.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_REPORT,
      groupOrder = "30",
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "report_format")
  private LintReportFormat reportFormat;

  public ActionRunLinter() {
    this("");
  }

  public ActionRunLinter(String name) {
    super(name, "");
    includeMetadata = true;
    minimumSeverity = LintSeverity.Level.INFO;
    failOn = LintSeverity.FailOn.ERROR;
    reportFormat = LintReportFormat.SARIF;
  }

  @Override
  public Result execute(Result result, int nr) {
    result.setResult(false);
    // Cleared first: after a run that fails with an error, the actions that follow must not read
    // the counts and the report of an earlier run.
    exportVariables("", "", "", "");
    try {
      LintRun run = new LintRun();
      File targetFile = localFile(resolveTarget(), "ActionRunLinter.Error.TargetNotLocal");
      run.setTarget(targetFile);
      run.setIncludeMetadata(includeMetadata);
      if (!Utils.isEmpty(resolve(configFile))) {
        run.setConfigFile(localFile(resolve(configFile), "ActionRunLinter.Error.ConfigNotLocal"));
      }
      run.setSeverityFilter(minimumSeverity);
      run.setFailOn(failOn == null ? LintSeverity.FailOn.ERROR : failOn);
      run.setMaxWarnings(resolveMaxWarnings());
      run.setBaseline(readBaseline());

      IVariables lintVariables = new Variables();
      lintVariables.copyFrom(this);
      LintRun.Outcome outcome =
          run.execute(metadataProviderFor(targetFile, lintVariables), lintVariables);

      String reportName = writeReport(run, outcome, result);
      result.setRows(rowsOf(outcome.getShown()));
      exportVariables(
          String.valueOf(outcome.count(LintSeverity.Level.ERROR)),
          String.valueOf(outcome.count(LintSeverity.Level.WARNING)),
          String.valueOf(outcome.count(LintSeverity.Level.INFO)),
          reportName);

      logBasic(
          BaseMessages.getString(
              PKG,
              "ActionRunLinter.Log.Summary",
              targetFile.getPath(),
              outcome.count(LintSeverity.Level.ERROR),
              outcome.count(LintSeverity.Level.WARNING),
              outcome.count(LintSeverity.Level.INFO)));
      if (outcome.getBaselineHidden() > 0) {
        logBasic(
            BaseMessages.getString(
                PKG, "ActionRunLinter.Log.Baseline", outcome.getBaselineHidden()));
      }
      if (isDetailed()) {
        for (LintResult finding : outcome.getShown()) {
          logDetailed(finding.toString());
        }
      }

      if (outcome.isFailedOnSeverity()) {
        logError(
            BaseMessages.getString(PKG, "ActionRunLinter.Log.FailedOnSeverity", run.getFailOn()));
      } else if (outcome.isFailedOnWarnings()) {
        logError(
            BaseMessages.getString(
                PKG,
                "ActionRunLinter.Log.FailedOnWarnings",
                outcome.count(LintSeverity.Level.WARNING),
                run.getMaxWarnings()));
      }
      result.setResult(!outcome.isFailed());
    } catch (Exception e) {
      // A broken rule pack or hop-lint.yml says what is wrong with it in the message.
      logError(BaseMessages.getString(PKG, "ActionRunLinter.Log.Error", e.getMessage()), e);
      result.setNrErrors(1);
    }
    return result;
  }

  /** The target, or the current project when none is set. */
  private String resolveTarget() throws HopException {
    String resolved = resolve(target);
    if (Utils.isEmpty(resolved)) {
      resolved = getVariable("PROJECT_HOME");
    }
    if (Utils.isEmpty(resolved)) {
      throw new HopException(BaseMessages.getString(PKG, "ActionRunLinter.Error.NoTarget"));
    }
    return resolved;
  }

  /**
   * The linter reads pipelines, workflows and hop-lint.yml from the local file system, so a target
   * on another one is refused with a message rather than linted as nothing.
   */
  private File localFile(String name, String errorKey) throws HopException {
    FileObject fileObject = HopVfs.getFileObject(name, this);
    if (!fileObject.getName().getRootURI().startsWith("file:")) {
      throw new HopException(BaseMessages.getString(PKG, errorKey, name));
    }
    return new File(HopVfs.getFilename(fileObject));
  }

  private int resolveMaxWarnings() throws HopException {
    String resolved = resolve(maxWarnings);
    if (Utils.isEmpty(resolved)) {
      return -1;
    }
    try {
      int parsed = Integer.parseInt(resolved.trim());
      if (parsed >= 0) {
        return parsed;
      }
    } catch (NumberFormatException e) {
      // Reported below, with the value.
    }
    throw new HopException(
        BaseMessages.getString(PKG, "ActionRunLinter.Error.MaxWarnings", resolved));
  }

  /**
   * The baseline, or null. A missing file fails the action, as it fails {@code hop lint}: treating
   * it as empty would fail on accepted findings, and treating it as complete would pass new ones.
   */
  private LintBaseline readBaseline() throws Exception {
    String resolved = resolve(baselineFile);
    if (Utils.isEmpty(resolved)) {
      return null;
    }
    FileObject fileObject = HopVfs.getFileObject(resolved, this);
    if (!fileObject.exists()) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionRunLinter.Error.BaselineNotFound", resolved));
    }
    try (InputStream in = HopVfs.getInputStream(fileObject)) {
      return LintBaseline.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8), resolved);
    }
  }

  /**
   * The workflow's metadata when the target lies in its project; otherwise as {@code hop lint}
   * finds it for a folder outside the project in use.
   */
  private IHopMetadataProvider metadataProviderFor(File targetFile, IVariables variables) {
    String projectHome = variables.getVariable("PROJECT_HOME");
    boolean inProject =
        !Utils.isEmpty(projectHome) && LintPathUtils.isWithin(targetFile, new File(projectHome));
    return LintRun.metadataProviderFor(
        targetFile, variables, inProject ? getMetadataProvider() : null);
  }

  /**
   * Write the report and add it to the result files, so a Mail action can attach it.
   *
   * @return the report's name, or an empty string when none is written
   */
  private String writeReport(LintRun run, LintRun.Outcome outcome, Result result) throws Exception {
    String resolved = resolve(reportFile);
    if (Utils.isEmpty(resolved)) {
      return "";
    }
    LintReportFormat format = reportFormat == null ? LintReportFormat.SARIF : reportFormat;
    String report =
        format == LintReportFormat.TEXT
            ? LintReportWriter.renderText(outcome.getShown(), true)
            : run.renderReport(outcome.getShown(), format);

    FileObject fileObject = HopVfs.getFileObject(resolved, this);
    FileObject parent = fileObject.getParent();
    if (parent != null && !parent.exists()) {
      parent.createFolder();
    }
    try (OutputStream out = HopVfs.getOutputStream(fileObject, false)) {
      out.write(report.getBytes(StandardCharsets.UTF_8));
    }
    result
        .getResultFiles()
        .put(
            fileObject.toString(),
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                fileObject,
                parentWorkflow == null ? null : parentWorkflow.getWorkflowName(),
                toString()));
    logBasic(BaseMessages.getString(PKG, "ActionRunLinter.Log.Report", resolved));
    return resolved;
  }

  /** The layout of the result rows, one per finding. */
  public static IRowMeta findingRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("severity"));
    rowMeta.addValueMeta(new ValueMetaString("rule_id"));
    rowMeta.addValueMeta(new ValueMetaString("rule_name"));
    rowMeta.addValueMeta(new ValueMetaString("message"));
    rowMeta.addValueMeta(new ValueMetaString("file"));
    rowMeta.addValueMeta(new ValueMetaString("element_type"));
    rowMeta.addValueMeta(new ValueMetaString("element_name"));
    return rowMeta;
  }

  static List<RowMetaAndData> rowsOf(List<LintResult> findings) {
    IRowMeta rowMeta = findingRowMeta();
    List<RowMetaAndData> rows = new ArrayList<>();
    for (LintResult finding : findings) {
      LintSourceRef source = finding.getSource();
      boolean named = source != null && source.hasName();
      rows.add(
          new RowMetaAndData(
              rowMeta,
              finding.getSeverity(),
              finding.getRuleId(),
              finding.getRuleName(),
              finding.getMessage(),
              finding.getFileName(),
              named ? source.getKind().name() : null,
              named ? source.getName() : null));
    }
    return rows;
  }

  /** The counts and the report for the actions that follow. */
  private void exportVariables(String errors, String warnings, String infos, String reportName) {
    setExported(VAR_ERRORS, errors);
    setExported(VAR_WARNINGS, warnings);
    setExported(VAR_INFOS, infos);
    setExported(VAR_REPORT_FILE, reportName);
  }

  private void setExported(String name, String value) {
    setVariable(name, value);
    if (parentWorkflow != null) {
      parentWorkflow.setVariable(name, value);
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }
}
