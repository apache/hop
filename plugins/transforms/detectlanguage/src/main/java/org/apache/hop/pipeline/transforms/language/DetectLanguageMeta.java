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
package org.apache.hop.pipeline.transforms.language;

import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_WARNING;
import static org.apache.hop.core.util.Utils.isEmpty;

import com.github.pemistahl.lingua.api.Language;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
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
    id = "DetectLanguage",
    image = "detectlanguage.svg",
    name = "i18n::DetectLanguage.Name",
    description = "i18n::DetectLanguage.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::DetectLanguage.Keyword",
    documentationUrl = "/pipeline/transforms/detectlanguage.html")
public class DetectLanguageMeta extends BaseTransformMeta<DetectLanguage, DetectLanguageData> {
  private static final Class<?> PKG = DetectLanguageMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DetectLanguage.Dialog";
  public static final String WIDGET_CORPUS_FIELD = "corpusField";
  public static final String WIDGET_PARALLELISM = "parallelism";
  public static final String WIDGET_LOW_ACCURACY = "lowAccuracy";
  public static final String WIDGET_LANGUAGES = "languages";

  private static final String GROUP_OPTIONS = "i18n::DetectLanguage.Group.Options";
  private static final String GROUP_LANGUAGES = "i18n::DetectLanguage.Group.Languages";

  @GuiWidgetElement(
      id = WIDGET_CORPUS_FIELD,
      order = "0100",
      type = GuiElementType.COMBO,
      label = "i18n::DetectLanguageDialog.CorpusFieldName.Label",
      toolTip = "i18n::DetectLanguageDialog.CorpusFieldName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_OPTIONS,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "corpusField",
      injectionKey = "FIELD",
      injectionKeyDescription = "DetectLanguage.Injection.CorpusField")
  private String corpusField;

  @GuiWidgetElement(
      id = WIDGET_PARALLELISM,
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "i18n::DetectLanguageDialog.Parallelism.Label",
      toolTip = "i18n::DetectLanguageDialog.Parallelism.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_OPTIONS,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "parallelism",
      injectionKey = "PARALLISM",
      injectionKeyDescription = "DetectLanguage.Injection.Parallelism")
  private boolean parallelism = false;

  /**
   * Lingua's low accuracy mode: only the trigram models are used. Faster and much lighter on
   * memory, less accurate on short text. Off for pipelines saved before the option existed.
   */
  @GuiWidgetElement(
      id = WIDGET_LOW_ACCURACY,
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::DetectLanguageDialog.LowAccuracy.Label",
      toolTip = "i18n::DetectLanguageDialog.LowAccuracy.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_OPTIONS,
      groupOrder = "0100")
  @HopMetadataProperty(
      key = "lowAccuracy",
      injectionKey = "LOW_ACCURACY",
      injectionKeyDescription = "DetectLanguage.Injection.LowAccuracy")
  private boolean lowAccuracy = false;

  /**
   * The languages the transform may answer. Empty means every spoken language Lingua knows, which
   * is what the transform did before the option existed.
   */
  @GuiWidgetElement(
      id = WIDGET_LANGUAGES,
      order = "0400",
      type = GuiElementType.TABLE,
      toolTip = "i18n::DetectLanguageDialog.Languages.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_LANGUAGES,
      groupOrder = "0200",
      tableRows = 8)
  @HopMetadataProperty(
      key = "language",
      groupKey = "languages",
      injectionGroupKey = "LANGUAGES",
      injectionGroupDescription = "DetectLanguage.Injection.Languages")
  private List<DetectLanguageChoice> languages = new ArrayList<>();

  public DetectLanguageMeta() {
    super();
  }

  @Override
  public void setDefault() {
    parallelism = false;
    lowAccuracy = false;
    languages = new ArrayList<>();
  }

  /** The names offered in the languages table: every language Lingua can detect. */
  public List<String> languageNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return Language.all().stream().map(Language::name).sorted().toList();
  }

  /**
   * Resolves the configured languages.
   *
   * @return the languages to detect, without duplicates and in the configured order; empty when
   *     none is configured, which means all spoken languages
   * @throws HopException when a name is not a Lingua language or when exactly one language is
   *     configured: detection needs at least two to choose from
   */
  public List<Language> resolveLanguages(IVariables variables) throws HopException {
    return resolveLanguages(variables, null);
  }

  /**
   * Resolves the configured languages, leaving out the names that still hold a variable.
   *
   * @param unresolved when not null, a name that still holds a variable after resolving is added
   *     here instead of failing: Verify does not know the values a pipeline only gets when it runs
   */
  List<Language> resolveLanguages(IVariables variables, List<String> unresolved)
      throws HopException {
    List<Language> resolved = new ArrayList<>();
    if (languages == null) {
      return resolved;
    }
    for (DetectLanguageChoice choice : languages) {
      String name = choice == null ? null : choice.getName();
      if (variables != null) {
        name = variables.resolve(name);
      }
      if (StringUtils.isBlank(name)) {
        continue;
      }
      if (unresolved != null && StringUtil.containsVariableToken(name)) {
        unresolved.add(name.trim());
        continue;
      }
      Language language = findLanguage(name.trim());
      if (language == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "DetectLanguage.Error.UnknownLanguage", name.trim()));
      }
      if (!resolved.contains(language)) {
        resolved.add(language);
      }
    }
    // With names still to be resolved, the number of languages is not known yet
    if (resolved.size() == 1 && (unresolved == null || unresolved.isEmpty())) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "DetectLanguage.Error.SingleLanguage", resolved.get(0).name()));
    }
    return resolved;
  }

  private static Language findLanguage(String name) {
    String upper = name.toUpperCase(Locale.ROOT);
    for (Language language : Language.all()) {
      if (language.name().equals(upper)) {
        return language;
      }
    }
    return null;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // name = getParentTransformMeta().getName()
    valueMetaString(r, name, "detected_language");
    valueMetaNumber(r, name, "detected_language_confidence");
  }

  private void valueMetaString(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaString(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
  }

  private void valueMetaNumber(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaNumber(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
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
    if (isEmpty(corpusField)) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DetectLanguageMeta.CheckResult.CorpusFieldMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DetectLanguageMeta.CheckResult.CorpusFieldOK"),
              transformMeta);
    }
    remarks.add(cr);

    try {
      List<String> unresolved = new ArrayList<>();
      List<Language> resolved = resolveLanguages(variables, unresolved);
      if (!unresolved.isEmpty()) {
        cr =
            new CheckResult(
                TYPE_RESULT_WARNING,
                BaseMessages.getString(
                    PKG,
                    "DetectLanguageMeta.CheckResult.LanguagesResolvedAtRuntime",
                    String.join(", ", unresolved)),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                TYPE_RESULT_OK,
                resolved.isEmpty()
                    ? BaseMessages.getString(PKG, "DetectLanguageMeta.CheckResult.AllLanguages")
                    : BaseMessages.getString(
                        PKG, "DetectLanguageMeta.CheckResult.LanguagesOK", resolved.size()),
                transformMeta);
      }
    } catch (HopException e) {
      cr = new CheckResult(TYPE_RESULT_ERROR, e.getMessage().trim(), transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "DetectLanguageMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DetectLanguageMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
