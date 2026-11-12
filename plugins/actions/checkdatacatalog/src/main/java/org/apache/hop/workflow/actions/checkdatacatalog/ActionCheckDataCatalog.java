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

package org.apache.hop.workflow.actions.checkdatacatalog;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.DatasetRef;
import org.apache.hop.datacatalog.IAssertionCapability;
import org.apache.hop.datacatalog.IAssertionCapability.AssertionResult;
import org.apache.hop.datacatalog.IAssertionCapability.Outcome;
import org.apache.hop.datacatalog.ICatalogCapability;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.datacatalog.IDeprecationCapability;
import org.apache.hop.datacatalog.IDeprecationCapability.Deprecation;
import org.apache.hop.datacatalog.IFreshnessCapability;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/**
 * Checks datasets in a data catalog before the workflow goes on: whether the catalog has them,
 * whether they are deprecated, whether their data is recent enough, and whether the checks the
 * catalog runs on them passed. The result is false when a dataset fails a check, so the workflow
 * can take another hop; a configuration it can't use is an error.
 */
@Action(
    id = "CHECK_DATA_CATALOG",
    name = "i18n::ActionCheckDataCatalog.Name",
    description = "i18n::ActionCheckDataCatalog.Description",
    image = "checkdatacatalog.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionCheckDataCatalog.Keywords",
    documentationUrl = "/workflow/actions/checkdatacatalog.html")
@GuiPlugin
@Getter
@Setter
public class ActionCheckDataCatalog extends ActionBase implements IAction {
  private static final Class<?> PKG = ActionCheckDataCatalog.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "CheckDataCatalog.Dialog.Options";
  public static final String GROUP_OPTIONS = "i18n::ActionCheckDataCatalog.Group.Options";

  @GuiWidgetElement(
      id = "catalogName",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DataCatalog.class,
      label = "i18n::ActionCheckDataCatalog.Catalog.Label",
      toolTip = "i18n::ActionCheckDataCatalog.Catalog.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "catalog",
      hopMetadataPropertyType = HopMetadataPropertyType.DATA_CATALOG)
  private String catalogName;

  @GuiWidgetElement(
      id = "checkDeprecation",
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ActionCheckDataCatalog.CheckDeprecation.Label",
      toolTip = "i18n::ActionCheckDataCatalog.CheckDeprecation.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(key = "check_deprecation")
  private boolean checkDeprecation;

  @GuiWidgetElement(
      id = "maxAge",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::ActionCheckDataCatalog.MaxAge.Label",
      toolTip = "i18n::ActionCheckDataCatalog.MaxAge.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(key = "max_age")
  private String maxAge;

  @GuiWidgetElement(
      id = "maxAgeUnit",
      order = "0400",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::ActionCheckDataCatalog.MaxAgeUnit.Label",
      toolTip = "i18n::ActionCheckDataCatalog.MaxAgeUnit.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(key = "max_age_unit")
  private CatalogAgeUnit maxAgeUnit;

  @GuiWidgetElement(
      id = "checkAssertions",
      order = "0500",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ActionCheckDataCatalog.CheckAssertions.Label",
      toolTip = "i18n::ActionCheckDataCatalog.CheckAssertions.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(key = "check_assertions")
  private boolean checkAssertions;

  @GuiWidgetElement(
      id = "unknownAnswer",
      order = "0600",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::ActionCheckDataCatalog.UnknownAnswer.Label",
      toolTip = "i18n::ActionCheckDataCatalog.UnknownAnswer.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(key = "unknown_answer")
  private CatalogUnknownAnswer unknownAnswer;

  /** The datasets to check, edited on their own tab of the dialog. */
  @HopMetadataProperty(groupKey = "datasets", key = "dataset")
  private List<CatalogDataset> datasets;

  /** The checks of the current dataset that were skipped because the catalog could not answer. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private int skippedChecks;

  public ActionCheckDataCatalog() {
    this("");
  }

  public ActionCheckDataCatalog(String name) {
    super(name, "");
    checkDeprecation = true;
    checkAssertions = true;
    maxAgeUnit = CatalogAgeUnit.HOURS;
    unknownAnswer = CatalogUnknownAnswer.FAIL;
    datasets = new ArrayList<>();
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult == null ? new Result() : previousResult;
    result.setResult(false);
    try {
      DataCatalog dataCatalog = loadCatalog();
      Duration activeMaxAge = maxAge();
      if (datasets == null || datasets.isEmpty()) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.NoDatasets"));
      }
      int failed = 0;
      int skipped = 0;
      try (IDataCatalog catalog = dataCatalog.connect(this, getMetadataProvider())) {
        for (CatalogDataset dataset : datasets) {
          if (getParentWorkflow() != null && getParentWorkflow().isStopped()) {
            logBasic(BaseMessages.getString(PKG, "ActionCheckDataCatalog.Log.Stopped"));
            return result;
          }
          DatasetRef ref = datasetRef(dataset);
          List<String> problems = check(catalog, ref, activeMaxAge);
          skipped += skippedChecks;
          if (problems.isEmpty() && skippedChecks > 0) {
            // Nothing failed, but not everything was checked: say so next to the failures
            logMinimal(
                BaseMessages.getString(
                    PKG,
                    "ActionCheckDataCatalog.Log.PassedSkipped",
                    ref.describe(),
                    Integer.toString(skippedChecks)));
          } else if (problems.isEmpty()) {
            logBasic(
                BaseMessages.getString(PKG, "ActionCheckDataCatalog.Log.Passed", ref.describe()));
          } else {
            failed++;
            logMinimal(
                BaseMessages.getString(
                    PKG,
                    "ActionCheckDataCatalog.Log.Failed",
                    ref.describe(),
                    String.join("; ", problems)));
          }
        }
      }
      logBasic(
          BaseMessages.getString(
              PKG,
              "ActionCheckDataCatalog.Log.Summary",
              Integer.toString(datasets.size() - failed),
              Integer.toString(datasets.size()),
              dataCatalog.getName()));
      if (skipped > 0) {
        logMinimal(
            BaseMessages.getString(
                PKG, "ActionCheckDataCatalog.Log.SkippedSummary", Integer.toString(skipped)));
      }
      result.setResult(failed == 0);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.Failed"), e);
      result.setNrErrors(result.getNrErrors() + 1);
      result.setResult(false);
    }
    return result;
  }

  private DataCatalog loadCatalog() throws HopException {
    String resolvedName = resolve(catalogName);
    if (StringUtils.isBlank(resolvedName)) {
      throw new HopException(BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.NoCatalog"));
    }
    IHopMetadataProvider metadataProvider = getMetadataProvider();
    DataCatalog dataCatalog =
        metadataProvider == null
            ? null
            : metadataProvider.getSerializer(DataCatalog.class).load(resolvedName);
    if (dataCatalog == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.MissingCatalog", resolvedName));
    }
    return dataCatalog;
  }

  /** The maximum age, or null when the data's age is not checked. */
  Duration maxAge() throws HopException {
    String resolved = StringUtils.trimToEmpty(resolve(maxAge));
    if (resolved.isEmpty()) {
      return null;
    }
    try {
      long amount = Long.parseLong(resolved);
      if (amount > 0) {
        return activeUnit().times(amount);
      }
    } catch (NumberFormatException e) {
      // Reported below
    }
    throw new HopException(
        BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.MaxAge", resolved));
  }

  private CatalogAgeUnit activeUnit() {
    return maxAgeUnit == null ? CatalogAgeUnit.HOURS : maxAgeUnit;
  }

  /** The dataset as Hop identifies it, so the catalog finds the dataset lineage wrote. */
  DatasetRef datasetRef(CatalogDataset dataset) throws HopException {
    String name = resolve(dataset.getName());
    if (StringUtils.isBlank(name)) {
      throw new HopException(BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.NoName"));
    }
    CatalogDatasetType type =
        dataset.getType() == null ? CatalogDatasetType.TABLE : dataset.getType();
    DatasetRef ref =
        switch (type) {
          case CATALOG_ID -> DatasetRef.ofCatalogId(name.trim());
          case FILE -> DatasetRef.ofFile(name, this);
          case TABLE -> {
            String connectionName = resolve(dataset.getConnection());
            DatabaseMeta databaseMeta =
                DatabaseMeta.loadDatabase(getMetadataProvider(), connectionName);
            if (databaseMeta == null) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, "ActionCheckDataCatalog.Error.MissingConnection", connectionName, name));
            }
            yield DatasetRef.ofTable(databaseMeta, this, resolve(dataset.getSchema()), name.trim());
          }
        };
    if (!ref.isComplete()) {
      // A connection without a host (an embedded database, a manual URL) has no name to look for
      throw new HopException(
          BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.NoIdentity", name));
    }
    return ref;
  }

  /** How many checks the last {@link #check} skipped because the catalog could not answer. */
  int skippedChecks() {
    return skippedChecks;
  }

  /**
   * What is wrong with one dataset; empty when it passes every check. The checks it skipped because
   * the catalog could not answer are counted in {@link #skippedChecks}.
   */
  List<String> check(IDataCatalog catalog, DatasetRef ref, Duration activeMaxAge) {
    List<String> problems = new ArrayList<>();
    skippedChecks = 0;
    CatalogAnswer<CatalogEntityId> resolved = catalog.resolve(ref);
    if (resolved instanceof CatalogAnswer.NotFound<CatalogEntityId>) {
      problems.add(BaseMessages.getString(PKG, "ActionCheckDataCatalog.Problem.NotFound"));
      return problems;
    }
    if (!(resolved instanceof CatalogAnswer.Known<CatalogEntityId> known)) {
      unknown(problems, "Exists", resolved);
      return problems;
    }
    CatalogEntityId id = known.value();

    if (checkDeprecation) {
      ask(catalog, IDeprecationCapability.class, c -> c.deprecation(id), problems, "Deprecation")
          .filter(Deprecation::deprecated)
          .ifPresent(
              deprecation ->
                  problems.add(
                      StringUtils.isBlank(deprecation.note())
                          ? BaseMessages.getString(PKG, "ActionCheckDataCatalog.Problem.Deprecated")
                          : BaseMessages.getString(
                              PKG,
                              "ActionCheckDataCatalog.Problem.DeprecatedNote",
                              deprecation.note().trim())));
    }

    if (activeMaxAge != null) {
      CatalogAnswer<Instant> lastUpdated =
          answer(catalog, IFreshnessCapability.class, c -> c.lastUpdated(id));
      if (lastUpdated instanceof CatalogAnswer.Known<Instant> update) {
        if (update.value().isBefore(Instant.now().minus(activeMaxAge))) {
          problems.add(
              BaseMessages.getString(
                  PKG,
                  "ActionCheckDataCatalog.Problem.TooOld",
                  update.value().toString(),
                  StringUtils.trim(resolve(maxAge)),
                  activeUnit().getDescription().toLowerCase(Locale.ROOT)));
        }
      } else {
        // Not knowing when the data changed doesn't tell that it is recent
        unknown(problems, "Freshness", lastUpdated);
      }
    }

    if (checkAssertions) {
      ask(
              catalog,
              IAssertionCapability.class,
              c -> c.latestAssertionResults(id),
              problems,
              "Assertions")
          .ifPresent(
              results -> {
                for (AssertionResult assertion : results) {
                  if (assertion.outcome() == Outcome.FAILED
                      || assertion.outcome() == Outcome.ERROR) {
                    problems.add(
                        BaseMessages.getString(
                            PKG,
                            "ActionCheckDataCatalog.Problem.Assertion",
                            StringUtils.defaultIfBlank(
                                assertion.description(), assertion.assertionId()),
                            assertion.outcome().name()));
                  }
                }
              });
    }
    return problems;
  }

  /**
   * The answer to a question, if the catalog knows it. Not found is a normal answer: no deprecation
   * recorded, no assertions. A question the catalog can't answer follows {@link #unknownAnswer}.
   */
  private <C extends ICatalogCapability, T> Optional<T> ask(
      IDataCatalog catalog,
      Class<C> capability,
      Function<C, CatalogAnswer<T>> question,
      List<String> problems,
      String check) {
    CatalogAnswer<T> answer = answer(catalog, capability, question);
    if (answer instanceof CatalogAnswer.Known<T> known) {
      return Optional.of(known.value());
    }
    if (answer instanceof CatalogAnswer.Unknown<T>) {
      unknown(problems, check, answer);
    }
    return Optional.empty();
  }

  private static <C extends ICatalogCapability, T> CatalogAnswer<T> answer(
      IDataCatalog catalog, Class<C> capability, Function<C, CatalogAnswer<T>> question) {
    return catalog
        .capability(capability)
        .map(question)
        .orElseGet(
            () ->
                CatalogAnswer.unknown(CatalogAnswer.Reason.NOT_SUPPORTED, catalog.getPluginName()));
  }

  /** The catalog could not answer a check: a problem, or a line in the log. */
  private void unknown(List<String> problems, String check, CatalogAnswer<?> answer) {
    String detail =
        answer instanceof CatalogAnswer.Unknown<?> unknown
            ? unknown.reason() + (unknown.detail() == null ? "" : ": " + unknown.detail())
            : BaseMessages.getString(PKG, "ActionCheckDataCatalog.Problem.NothingKnown");
    String message =
        BaseMessages.getString(
            PKG,
            "ActionCheckDataCatalog.Problem.Unknown",
            BaseMessages.getString(PKG, "ActionCheckDataCatalog.Check." + check),
            detail);
    if (unknownAnswer == CatalogUnknownAnswer.IGNORE) {
      skippedChecks++;
      logBasic(message);
    } else {
      problems.add(message);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (StringUtils.isBlank(variables.resolve(catalogName))) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.NoCatalog"),
              this));
    }
    if (datasets == null || datasets.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.NoDatasets"),
              this));
    }
    String resolved = StringUtils.trimToEmpty(variables.resolve(maxAge));
    // A variable that is not set yet can only be judged when the action runs
    if (!resolved.isEmpty() && !resolved.contains("${") && !isPositiveNumber(resolved)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ActionCheckDataCatalog.Error.MaxAge", resolved),
              this));
    }
  }

  private static boolean isPositiveNumber(String value) {
    try {
      return Long.parseLong(value) > 0;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
