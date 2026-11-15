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

package org.apache.hop.ai.engine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hop.ai.advisor.AiAdvisorInclusionChoice;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.IContextCapability;
import org.apache.hop.datacatalog.IContextCapability.DatasetContext;
import org.apache.hop.datacatalog.IContextCapability.Owner;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.datacatalog.IDeprecationCapability;
import org.apache.hop.datacatalog.IDeprecationCapability.Deprecation;
import org.apache.hop.datacatalog.PipelineDatasets;
import org.apache.hop.datacatalog.PipelineDatasets.TouchedDataset;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * What a data catalog says about the tables and files a pipeline reads and writes: descriptions,
 * owners, tags, glossary terms and deprecation, for the {@code data_catalog} section of the prompt.
 *
 * <p>The prompt is built on the UI thread, which must not wait for a catalog. {@link
 * #appendToPrompt} only lists the datasets and leaves a marker; {@link #fill} asks the catalog in
 * the background, just before the question is sent, and puts the answers in the marker's place.
 */
public final class AiAdvisorCatalogContext {

  /** The {@link AiAdvisorRequest} attribute holding what {@link #fill} has to look up. */
  static final String ATTR_PENDING = "ai.dataCatalogContext";

  static final String SECTION = "data_catalog";

  /** How many datasets are looked up; the others are only counted. */
  static final int MAX_DATASETS = 40;

  static final int MAX_CHARS = 30_000;

  /** What {@link #fill} looks up, and the marker it replaces. */
  record Pending(String marker, String catalogName, List<TouchedDataset> datasets) {}

  private AiAdvisorCatalogContext() {}

  /** The data catalogs of the project, to pick one from. */
  public static List<AiAdvisorInclusionChoice> choices(IHopMetadataProvider metadataProvider) {
    List<AiAdvisorInclusionChoice> choices = new ArrayList<>();
    if (metadataProvider == null) {
      return choices;
    }
    try {
      List<String> names =
          new ArrayList<>(metadataProvider.getSerializer(DataCatalog.class).listObjectNames());
      names.sort(String.CASE_INSENSITIVE_ORDER);
      for (String name : names) {
        choices.add(new AiAdvisorInclusionChoice(name, name));
      }
    } catch (Exception e) {
      // No data catalogs to offer
    }
    return choices;
  }

  /**
   * Adds the section with a marker for the catalog's answers when the inclusion is on and a catalog
   * is picked. Reads only the pipeline: call it on the UI thread with the rest of the prompt.
   */
  public static void appendToPrompt(
      StringBuilder prompt, AiAdvisorRequest request, PipelineMeta pipelineMeta) {
    if (prompt == null
        || request == null
        || pipelineMeta == null
        || !request.inclusionEnabled(AiAdvisorInclusions.DATA_CATALOG)) {
      return;
    }
    List<String> picked = request.selectedInclusionIds(AiAdvisorInclusions.DATA_CATALOG);
    if (picked == null || picked.isEmpty() || Utils.isEmpty(picked.get(0))) {
      return;
    }
    String catalogName = picked.get(0);
    List<TouchedDataset> datasets =
        PipelineDatasets.of(
            pipelineMeta,
            request.getVariables() != null ? request.getVariables() : new Variables());
    String marker = "hop-data-catalog-" + UUID.randomUUID();
    request.getAttributes().put(ATTR_PENDING, new Pending(marker, catalogName, datasets));
    StringBuilder placeholder = new StringBuilder(marker).append('\n');
    placeholder
        .append("What data catalog '")
        .append(catalogName)
        .append("' says about these datasets is read when the question is sent:");
    for (TouchedDataset dataset : datasets) {
      placeholder.append("\n- ").append(dataset.dataset().describe());
    }
    if (datasets.isEmpty()) {
      placeholder.append(" none, the pipeline declares no tables or files.");
    }
    AiTextUtil.appendSection(prompt, SECTION, placeholder.toString());
  }

  /**
   * Asks the catalog and puts its answers in the place of the marker. Runs in the background. A
   * catalog that can't be reached leaves a note saying so, not a failed question.
   */
  public static void fill(AiAdvisorRequest request, AiAdvisorPrompt prompt) {
    if (request == null
        || prompt == null
        || !(request.getAttributes().get(ATTR_PENDING) instanceof Pending pending)) {
      return;
    }
    String user = prompt.getUserPrompt();
    if (user == null) {
      return;
    }
    int start = user.indexOf(pending.marker());
    int end = user.indexOf("</" + SECTION + ">", start);
    if (start < 0 || end < 0) {
      return;
    }
    IVariables variables = request.getVariables();
    // JSON escapes a slash; unescaped, paths can be shortened and tags in a description broken
    String json = lookUp(pending, variables, request.getMetadataProvider()).replace("\\/", "/");
    String content =
        AiTextUtil.breakSectionTags(AiTextUtil.redactSecrets(AiTextUtil.truncate(json, MAX_CHARS)));
    if (variables != null) {
      content = AiTextUtil.shortenPaths(content, variables);
    }
    prompt.setUserPrompt(user.substring(0, start) + content.strip() + "\n" + user.substring(end));
  }

  @SuppressWarnings("unchecked")
  static String lookUp(
      Pending pending, IVariables variables, IHopMetadataProvider metadataProvider) {
    JSONObject root = new JSONObject();
    root.put("catalog", pending.catalogName());
    JSONArray datasets = new JSONArray();
    root.put("datasets", datasets);
    if (pending.datasets().isEmpty()) {
      return root.toJSONString();
    }
    IDataCatalog catalog = null;
    try {
      DataCatalog meta =
          metadataProvider == null
              ? null
              : metadataProvider.getSerializer(DataCatalog.class).load(pending.catalogName());
      if (meta == null) {
        root.put("error", "data catalog " + pending.catalogName() + " doesn't exist");
        return root.toJSONString();
      }
      catalog = meta.connect(variables, metadataProvider);
      IContextCapability context = catalog.capability(IContextCapability.class).orElse(null);
      IDeprecationCapability deprecation =
          catalog.capability(IDeprecationCapability.class).orElse(null);
      int count = 0;
      for (TouchedDataset touched : pending.datasets()) {
        if (count++ >= MAX_DATASETS) {
          root.put("notLookedUp", pending.datasets().size() - MAX_DATASETS);
          break;
        }
        JSONObject dataset = describe(touched);
        datasets.add(dataset);
        CatalogAnswer<CatalogEntityId> resolved = catalog.resolve(touched.dataset());
        if (resolved instanceof CatalogAnswer.Unknown<CatalogEntityId> unknown) {
          dataset.put("unknown", unknown.detail());
          if (unknown.reason() == CatalogAnswer.Reason.UNREACHABLE
              || unknown.reason() == CatalogAnswer.Reason.UNAUTHORIZED
              || unknown.reason() == CatalogAnswer.Reason.TIMEOUT) {
            // Every other dataset would wait for the same answer
            root.put("error", "the catalog can't be read: " + unknown.detail());
            break;
          }
          continue;
        }
        if (!(resolved instanceof CatalogAnswer.Known<CatalogEntityId> known)) {
          dataset.put("inCatalog", Boolean.FALSE);
          continue;
        }
        dataset.put("inCatalog", Boolean.TRUE);
        if (context != null
            && context.context(known.value()) instanceof CatalogAnswer.Known<DatasetContext> c) {
          putContext(dataset, c.value());
        }
        if (deprecation != null
            && deprecation.deprecation(known.value()) instanceof CatalogAnswer.Known<Deprecation> d
            && d.value().deprecated()) {
          dataset.put("deprecated", Boolean.TRUE);
          putIfPresent(dataset, "deprecationNote", d.value().note());
          Instant decommission = d.value().decommissionTime();
          if (decommission != null) {
            dataset.put("decommissionTime", decommission.toString());
          }
        }
      }
    } catch (Exception e) {
      root.put("error", "the catalog can't be read: " + e.getMessage());
    } finally {
      if (catalog != null) {
        catalog.close();
      }
    }
    return root.toJSONString();
  }

  @SuppressWarnings("unchecked")
  private static JSONObject describe(TouchedDataset touched) {
    JSONObject dataset = new JSONObject();
    dataset.put("namespace", touched.dataset().namespace());
    dataset.put("name", touched.dataset().name());
    if (!touched.readBy().isEmpty()) {
      JSONArray readBy = new JSONArray();
      readBy.addAll(touched.readBy());
      dataset.put("readBy", readBy);
    }
    if (!touched.writtenBy().isEmpty()) {
      JSONArray writtenBy = new JSONArray();
      writtenBy.addAll(touched.writtenBy());
      dataset.put("writtenBy", writtenBy);
    }
    return dataset;
  }

  @SuppressWarnings("unchecked")
  private static void putContext(JSONObject dataset, DatasetContext context) {
    putIfPresent(dataset, "description", context.description());
    if (!context.owners().isEmpty()) {
      JSONArray owners = new JSONArray();
      for (Owner owner : context.owners()) {
        JSONObject entry = new JSONObject();
        entry.put("name", owner.name());
        putIfPresent(entry, "role", owner.role());
        owners.add(entry);
      }
      dataset.put("owners", owners);
    }
    if (!context.tags().isEmpty()) {
      JSONArray tags = new JSONArray();
      tags.addAll(context.tags());
      dataset.put("tags", tags);
    }
    if (!context.terms().isEmpty()) {
      JSONArray terms = new JSONArray();
      terms.addAll(context.terms());
      dataset.put("glossaryTerms", terms);
    }
  }

  @SuppressWarnings("unchecked")
  private static void putIfPresent(JSONObject object, String key, String value) {
    if (!Utils.isEmpty(value)) {
      object.put(key, value);
    }
  }
}
