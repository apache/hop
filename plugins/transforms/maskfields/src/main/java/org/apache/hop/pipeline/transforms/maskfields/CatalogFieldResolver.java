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

package org.apache.hop.pipeline.transforms.maskfields;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.DatasetRef;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.datacatalog.ISchemaCapability;
import org.apache.hop.datacatalog.ISchemaCapability.CatalogField;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Asks the data catalog of a Mask fields transform which fields of its dataset carry the tags and
 * terms of its rules. When the catalog can't give a clear answer, nothing is guessed: the transform
 * does not start, so no field it should mask goes through unmasked.
 */
public class CatalogFieldResolver {
  private static final Class<?> PKG = CatalogFieldResolver.class;

  /**
   * A field the catalog labels with a tag or term of a rule.
   *
   * @param fieldName the field name in the catalog
   * @param patternName the masking pattern of the first rule whose tag or term it carries
   * @param label that tag or term
   */
  public record Match(String fieldName, String patternName, String label) {}

  private final IVariables variables;
  private final ILogChannel log;
  private final MaskFieldsMeta meta;
  private final IHopMetadataProvider metadataProvider;

  public CatalogFieldResolver(
      IVariables variables,
      ILogChannel log,
      MaskFieldsMeta meta,
      IHopMetadataProvider metadataProvider) {
    this.variables = variables;
    this.log = log;
    this.meta = meta;
    this.metadataProvider = metadataProvider;
  }

  /**
   * The labelled fields by lower-case name.
   *
   * @throws HopException when there is no catalog or dataset, or the catalog doesn't answer
   */
  public Map<String, Match> resolve() throws HopException {
    String catalogName = variables.resolve(meta.getCatalogName());
    if (StringUtils.isBlank(catalogName)) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Check.TagRulesWithoutCatalog"));
    }
    DataCatalog dataCatalog =
        metadataProvider == null
            ? null
            : metadataProvider.getSerializer(DataCatalog.class).load(catalogName);
    if (dataCatalog == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Error.MissingCatalog", catalogName));
    }
    // A tag whose variable isn't set would match nothing and leave every field unmasked
    List<MaskTagRule> rules = meta.activeTagRules();
    List<String> labels = new ArrayList<>();
    for (MaskTagRule rule : rules) {
      String label = resolvedTag(variables, rule);
      if (label == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "MaskFields.Error.UnresolvedTag", rule.getTag()));
      }
      labels.add(label);
    }
    DatasetRef ref = meta.catalogDataset().toRef(variables, metadataProvider);
    try (IDataCatalog catalog = dataCatalog.connect(variables, metadataProvider)) {
      CatalogEntityId id = known(catalog.resolve(ref), ref, catalogName);
      ISchemaCapability schemaCapability =
          catalog
              .capability(ISchemaCapability.class)
              .orElseThrow(
                  () ->
                      new HopException(
                          BaseMessages.getString(
                              PKG, "MaskFields.Error.NoSchemaSupport", catalogName)));
      List<CatalogField> fields = known(schemaCapability.schema(id), ref, catalogName);

      Map<String, Match> matches = new LinkedHashMap<>();
      for (CatalogField field : fields) {
        for (int i = 0; i < rules.size(); i++) {
          if (field.isLabelledWith(labels.get(i))) {
            matches.putIfAbsent(
                field.name().toLowerCase(Locale.ROOT),
                new Match(field.name(), rules.get(i).getPatternName(), labels.get(i)));
            break;
          }
        }
      }
      log.logBasic(
          BaseMessages.getString(
              PKG,
              "MaskFields.Log.CatalogFields",
              catalogName,
              ref.describe(),
              Integer.toString(matches.size()),
              Integer.toString(fields.size())));
      return matches;
    }
  }

  /** The tag or term of a rule with its variables resolved, or null when one isn't set. */
  static String resolvedTag(IVariables variables, MaskTagRule rule) {
    String label = StringUtils.trimToNull(variables.resolve(rule.getTag()));
    return label == null || label.contains("${") ? null : label;
  }

  /** The answer, or an exception saying why there is none. */
  private <T> T known(CatalogAnswer<T> answer, DatasetRef ref, String catalogName)
      throws HopException {
    return switch (answer) {
      case CatalogAnswer.Known<T> known -> known.value();
      case CatalogAnswer.NotFound<T> notFound ->
          throw new HopException(
              BaseMessages.getString(
                  PKG, "MaskFields.Error.CatalogNotFound", catalogName, ref.describe()));
      case CatalogAnswer.Unknown<T> unknown ->
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "MaskFields.Error.CatalogUnknown",
                  catalogName,
                  ref.describe(),
                  unknown.reason().name(),
                  StringUtils.defaultString(unknown.detail())));
    };
  }
}
