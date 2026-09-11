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

package org.apache.hop.git.pipeline.transforms.gitinput;

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
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
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.git.provider.GitConnection;
import org.apache.hop.git.provider.GitInputFields;
import org.apache.hop.git.provider.GitListOptions;
import org.apache.hop.git.provider.GitResourceType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "GitInput",
    name = "i18n::GitInput.Name",
    description = "i18n::GitInput.Description",
    image = "git.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::GitInput.keyword",
    documentationUrl = "/pipeline/transforms/gitinput.html")
@GuiPlugin
public class GitInputMeta extends BaseTransformMeta<GitInput, GitInputData> {

  private static final Class<?> PKG = GitInputMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "GIT_INPUT_DIALOG_OPTIONS";

  public static final String WIDGET_SOURCE = "source";
  public static final String WIDGET_CONNECTION = "connectionName";
  public static final String WIDGET_LOCAL_REPOSITORY_PATH = "localRepositoryPath";
  public static final String WIDGET_OWNER = "owner";
  public static final String WIDGET_REPOSITORY = "repository";
  public static final String WIDGET_BROWSE_REPOSITORY = "browseRepository";
  public static final String WIDGET_BRANCH = "branch";
  public static final String WIDGET_BROWSE_BRANCH = "browseBranch";
  public static final String WIDGET_RESOURCE_TYPE = "resourceType";
  public static final String WIDGET_STATE = "state";
  public static final String WIDGET_SINCE = "since";
  public static final String WIDGET_PAGE_SIZE = "pageSize";
  public static final String WIDGET_MAX_PAGES = "maxPages";
  public static final String WIDGET_INCLUDE_RAW_JSON = "includeRawJson";

  /**
   * Which Browse button was last pressed, consumed by {@link GitInputDialog}.
   *
   * <p>Not a setting: {@link org.apache.hop.ui.core.gui.GuiCompositeWidgets} calls its modified
   * listener twice for one button press (once directly, once through asyncExec), but invokes the
   * annotated method below exactly once. Marking the press here lets the dialog act on the first
   * notification and ignore the duplicate, instead of opening the browser twice.
   */
  private transient String pendingBrowse;

  private static final String GROUP_SOURCE = "Source";
  private static final String GROUP_REPOSITORY = "Repository";
  private static final String GROUP_CONTENT = "Content";

  @GuiWidgetElement(
      id = WIDGET_SOURCE,
      order = "0100",
      type = GuiElementType.COMBO,
      comboValuesMethod = "getSourceLabels",
      label = "i18n::GitInputDialog.Source.Label",
      toolTip = "i18n::GitInputDialog.Source.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SOURCE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "source", injectionKey = "SOURCE")
  private String source = GitInputSource.REMOTE.name();

  @GuiWidgetElement(
      id = WIDGET_CONNECTION,
      order = "0200",
      type = GuiElementType.METADATA,
      metadata = GitConnection.class,
      label = "i18n::GitInputDialog.Connection.Label",
      toolTip = "i18n::GitInputDialog.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SOURCE,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTION",
      hopMetadataPropertyType = HopMetadataPropertyType.GIT_CONNECTION)
  private String connectionName;

  @GuiWidgetElement(
      id = WIDGET_LOCAL_REPOSITORY_PATH,
      order = "0300",
      type = GuiElementType.FOLDER,
      label = "i18n::GitInputDialog.LocalRepository.Label",
      toolTip = "i18n::GitInputDialog.LocalRepository.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SOURCE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "local_repository_path", injectionKey = "LOCAL_REPOSITORY_PATH")
  private String localRepositoryPath;

  @GuiWidgetElement(
      id = WIDGET_OWNER,
      order = "0100",
      type = GuiElementType.COMBO,
      label = "i18n::GitInputDialog.Owner.Label",
      toolTip = "i18n::GitInputDialog.Owner.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "0200")
  @HopMetadataProperty(key = "owner", injectionKey = "OWNER")
  private String owner;

  @GuiWidgetElement(
      id = WIDGET_REPOSITORY,
      order = "0200",
      type = GuiElementType.COMBO,
      label = "i18n::GitInputDialog.Repository.Label",
      toolTip = "i18n::GitInputDialog.Repository.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "0200")
  @HopMetadataProperty(key = "repository", injectionKey = "REPOSITORY")
  private String repository;

  /**
   * Annotated so the Repository tab shows a Browse button. The list dialog is opened from {@link
   * GitInputDialog} so it can use the transform dialog shell and the repository names already
   * loaded in the background.
   */
  @GuiWidgetElement(
      id = WIDGET_BROWSE_REPOSITORY,
      order = "0300",
      type = GuiElementType.BUTTON,
      label = "i18n::GitInputDialog.Browse.Label",
      toolTip = "i18n::GitInputDialog.Repository.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "0200")
  public void browseRepository(Object object) {
    if (object instanceof GitInputMeta meta) {
      meta.setPendingBrowse(WIDGET_BROWSE_REPOSITORY);
    }
  }

  @GuiWidgetElement(
      id = WIDGET_BRANCH,
      order = "0400",
      type = GuiElementType.COMBO,
      label = "i18n::GitInputDialog.Branch.Label",
      toolTip = "i18n::GitInputDialog.Branch.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "0200")
  @HopMetadataProperty(key = "branch", injectionKey = "BRANCH")
  private String branch;

  /** Annotated so the Repository tab shows a Browse button for branches. */
  @GuiWidgetElement(
      id = WIDGET_BROWSE_BRANCH,
      order = "0500",
      type = GuiElementType.BUTTON,
      label = "i18n::GitInputDialog.Browse.Label",
      toolTip = "i18n::GitInputDialog.Branch.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "0200")
  public void browseBranch(Object object) {
    if (object instanceof GitInputMeta meta) {
      meta.setPendingBrowse(WIDGET_BROWSE_BRANCH);
    }
  }

  @GuiWidgetElement(
      id = WIDGET_RESOURCE_TYPE,
      order = "0100",
      type = GuiElementType.COMBO,
      comboValuesMethod = "getResourceTypeLabels",
      label = "i18n::GitInputDialog.ResourceType.Label",
      toolTip = "i18n::GitInputDialog.ResourceType.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONTENT,
      groupOrder = "0300")
  @HopMetadataProperty(key = "resource_type", injectionKey = "RESOURCE_TYPE")
  private String resourceType = "COMMITS";

  @GuiWidgetElement(
      id = WIDGET_STATE,
      order = "0200",
      type = GuiElementType.COMBO,
      comboValuesMethod = "getStateLabels",
      label = "i18n::GitInputDialog.State.Label",
      toolTip = "i18n::GitInputDialog.State.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONTENT,
      groupOrder = "0300")
  @HopMetadataProperty(key = "state", injectionKey = "STATE")
  private String state = "all";

  @GuiWidgetElement(
      id = WIDGET_SINCE,
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::GitInputDialog.Since.Label",
      toolTip = "i18n::GitInputDialog.Since.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONTENT,
      groupOrder = "0300")
  @HopMetadataProperty(key = "since", injectionKey = "SINCE")
  private String since;

  @GuiWidgetElement(
      id = WIDGET_PAGE_SIZE,
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::GitInputDialog.PageSize.Label",
      toolTip = "i18n::GitInputDialog.PageSize.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONTENT,
      groupOrder = "0300")
  @HopMetadataProperty(key = "page_size", injectionKey = "PAGE_SIZE")
  private String pageSize = "50";

  @GuiWidgetElement(
      id = WIDGET_MAX_PAGES,
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::GitInputDialog.MaxPages.Label",
      toolTip = "i18n::GitInputDialog.MaxPages.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONTENT,
      groupOrder = "0300")
  @HopMetadataProperty(key = "max_pages", injectionKey = "MAX_PAGES")
  private String maxPages = "20";

  @GuiWidgetElement(
      id = WIDGET_INCLUDE_RAW_JSON,
      order = "0600",
      type = GuiElementType.CHECKBOX,
      label = "i18n::GitInputDialog.IncludeRawJson.Label",
      toolTip = "i18n::GitInputDialog.IncludeRawJson.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONTENT,
      groupOrder = "0300")
  @HopMetadataProperty(key = "include_raw_json", injectionKey = "INCLUDE_RAW_JSON")
  private boolean includeRawJson = true;

  public GitInputMeta() {
    super();
  }

  /** Combo values for the Source widget. */
  public List<String> getSourceLabels(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return Arrays.asList(GitInputSource.labels());
  }

  /**
   * Combo values for the Resource type widget. A local repository is walked with JGit and only
   * exposes commits and their changed files; the issue and pull request types need a provider API.
   */
  public List<String> getResourceTypeLabels(
      ILogChannel log, IHopMetadataProvider metadataProvider) {
    boolean isLocal = GitInputSource.LOCAL.name().equals(StringUtils.trimToEmpty(source));
    return Arrays.asList(isLocal ? GitResourceType.localLabels() : GitResourceType.remoteLabels());
  }

  /** Combo values for the State widget. */
  public List<String> getStateLabels(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of("all", "open", "closed");
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    for (int i = 0; i < GitInputFields.fieldCount(includeRawJson); i++) {
      IValueMeta valueMeta;
      try {
        valueMeta =
            ValueMetaFactory.createValueMeta(
                GitInputFields.FIELD_NAMES[i], GitInputFields.FIELD_TYPES[i]);
      } catch (HopPluginException e) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "GitInputMeta.Error.CreateValueMeta", GitInputFields.FIELD_NAMES[i]),
            e);
      }
      if (GitInputFields.FIELD_LENGTHS[i] > 0) {
        valueMeta.setLength(GitInputFields.FIELD_LENGTHS[i]);
      }
      valueMeta.setOrigin(name);
      rowMeta.addValueMeta(valueMeta);
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

    if (input != null && input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.UnexpectedInput"),
              transformMeta));
    }

    GitInputSource inputSource;
    try {
      inputSource = GitInputSource.fromStored(variables.resolve(source));
    } catch (IllegalArgumentException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.UnknownSource", source),
              transformMeta));
      return;
    }

    GitResourceType type;
    try {
      type = GitResourceType.fromStored(variables.resolve(resourceType));
    } catch (IllegalArgumentException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "GitInputMeta.CheckResult.UnknownResourceType", resourceType),
              transformMeta));
      return;
    }

    if (inputSource == GitInputSource.LOCAL) {
      checkLocalSource(remarks, transformMeta, type);
    } else {
      checkRemoteSource(remarks, transformMeta, variables, metadataProvider, type);
    }
    checkPaging(remarks, transformMeta, variables, inputSource);
  }

  private void checkLocalSource(
      List<ICheckResult> remarks, TransformMeta transformMeta, GitResourceType type) {
    if (StringUtils.isEmpty(localRepositoryPath)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.LocalPathMissing"),
              transformMeta));
    }
    if (type != GitResourceType.COMMITS && type != GitResourceType.COMMIT_FILES) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "GitInputMeta.CheckResult.LocalResourceTypeUnsupported", type.name()),
              transformMeta));
    }
  }

  private void checkRemoteSource(
      List<ICheckResult> remarks,
      TransformMeta transformMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      GitResourceType type) {

    if (StringUtils.isEmpty(connectionName)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.ConnectionMissing"),
              transformMeta));
    } else if (metadataProvider != null) {
      String resolved = variables.resolve(connectionName);
      try {
        if (!metadataProvider.getSerializer(GitConnection.class).exists(resolved)) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "GitInputMeta.CheckResult.ConnectionNotFound", resolved),
                  transformMeta));
        }
      } catch (HopException e) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(
                    PKG, "GitInputMeta.CheckResult.ConnectionNotVerified", resolved),
                transformMeta));
      }
    }

    if (StringUtils.isEmpty(owner) || StringUtils.isEmpty(repository)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.RepositoryMissing"),
              transformMeta));
    }

    if (type == GitResourceType.COMMIT_FILES) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.CommitFilesRemote"),
              transformMeta));
    }
  }

  private void checkPaging(
      List<ICheckResult> remarks,
      TransformMeta transformMeta,
      IVariables variables,
      GitInputSource inputSource) {

    Integer resolvedPageSize = resolvedNumber(variables, pageSize);
    if (resolvedPageSize != null && resolvedPageSize <= 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.PageSizeInvalid", pageSize),
              transformMeta));
    }

    if (inputSource != GitInputSource.REMOTE) {
      return;
    }

    // 0 is valid here and means "every page"; only a negative or unparseable value is an error.
    Integer resolvedMaxPages = resolvedNumber(variables, maxPages);
    if (resolvedMaxPages != null && resolvedMaxPages < 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.MaxPagesInvalid", maxPages),
              transformMeta));
      return;
    }

    int effectivePageSize =
        resolvedPageSize == null || resolvedPageSize <= 0
            ? GitListOptions.DEFAULT_PAGE_SIZE
            : resolvedPageSize;
    int effectiveMaxPages =
        resolvedMaxPages == null || resolvedMaxPages < 0
            ? GitListOptions.DEFAULT_MAX_PAGES
            : resolvedMaxPages;
    if (effectiveMaxPages == GitListOptions.UNLIMITED_MAX_PAGES) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_COMMENT,
              BaseMessages.getString(PKG, "GitInputMeta.CheckResult.RowCapUnlimited"),
              transformMeta));
      return;
    }
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(
                PKG,
                "GitInputMeta.CheckResult.RowCap",
                String.valueOf((long) effectivePageSize * effectiveMaxPages)),
            transformMeta));
  }

  /**
   * Resolves a numeric setting for validation. Returns {@code null} when the value cannot be judged
   * at design time - either because it is empty (the runtime default applies) or because it still
   * holds an unresolved variable that will only be set when the pipeline runs. A value that is not
   * a number at all resolves to -1, which every caller rejects.
   */
  private static Integer resolvedNumber(IVariables variables, String value) {
    if (StringUtils.isEmpty(value)) {
      return null;
    }
    String resolved = variables.resolve(value);
    if (StringUtils.isEmpty(resolved) || resolved.contains("${")) {
      return null;
    }
    return Const.toInt(resolved, -1);
  }
}
