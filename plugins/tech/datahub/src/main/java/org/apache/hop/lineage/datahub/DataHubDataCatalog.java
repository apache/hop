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

package org.apache.hop.lineage.datahub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogAnswer.Reason;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.DatasetRef;
import org.apache.hop.datacatalog.IAssertionCapability;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.datacatalog.IDeprecationCapability;
import org.apache.hop.datacatalog.IFreshnessCapability;
import org.apache.hop.datacatalog.ISchemaCapability;
import org.apache.hop.datacatalog.plugin.DataCatalogPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * DataHub as a data catalog: Hop reads dataset metadata from DataHub's GraphQL API with a personal
 * access token. A table or file resolves to the dataset URN the DataHub lineage sink writes for it.
 * The queries work on DataHub 1.0 and later. Experimental.
 */
@DataCatalogPlugin(
    id = "datahub",
    name = "DataHub",
    description = "Reads dataset metadata from DataHub")
@GuiPlugin(description = "DataHub data catalog")
@Getter
@Setter
public class DataHubDataCatalog
    implements IDataCatalog,
        IDeprecationCapability,
        IFreshnessCapability,
        IAssertionCapability,
        ISchemaCapability {

  static final String DEFAULT_ENVIRONMENT = "PROD";
  static final String DEFAULT_CONNECT_TIMEOUT_MS = "5000";
  static final String DEFAULT_READ_TIMEOUT_MS = "30000";

  /** How many assertions of one dataset are read. */
  static final int MAX_ASSERTIONS = 100;

  /** How many of the latest operations are read to find the last update. */
  static final int MAX_OPERATIONS = 10;

  private static final String PING_QUERY =
      "query { me { corpUser { username } } appConfig { appVersion } }";

  private static final String EXISTS_QUERY =
      "query hopDatasetExists($urn: String!) { dataset(urn: $urn) { urn exists } }";

  private static final String DEPRECATION_QUERY =
      "query hopDatasetDeprecation($urn: String!) {"
          + " dataset(urn: $urn) { deprecation { deprecated note decommissionTime } } }";

  private static final String OPERATIONS_QUERY =
      "query hopDatasetOperations($urn: String!, $limit: Int) {"
          + " dataset(urn: $urn) { operations(limit: $limit) { lastUpdatedTimestamp } } }";

  /** The tags and terms of a field, with every name DataHub gives them. */
  private static final String LABELS =
      "globalTags { tags { tag { urn name properties { name } } } }"
          + " glossaryTerms { terms { term { urn name hierarchicalName properties { name }"
          + " parentNodes { nodes { properties { name } } } } } }";

  private static final String SCHEMA_QUERY =
      "query hopDatasetSchema($urn: String!) { dataset(urn: $urn) {"
          + " schemaMetadata { fields { fieldPath nativeDataType type nullable "
          + LABELS
          + " } }"
          + " editableSchemaMetadata { editableSchemaFieldInfo { fieldPath "
          + LABELS
          + " } } } }";

  private static final String ASSERTIONS_QUERY =
      "query hopDatasetAssertions($urn: String!, $count: Int) {"
          + " dataset(urn: $urn) { assertions(start: 0, count: $count) { total assertions {"
          + " urn info { description }"
          + " runEvents(status: COMPLETE, limit: 1) { runEvents { timestampMillis result { type } } }"
          + " } } } }";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** A type or version part of a DataHub v2 field path, with the dot after it. */
  private static final Pattern V2_PATH_PART = Pattern.compile("\\[[^\\]]*\\]\\.?");

  private String pluginId;
  private String pluginName;

  @GuiWidgetElement(
      id = "url",
      order = "010",
      parentId = DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DataHubDataCatalog.Url.Label",
      toolTip = "i18n::DataHubDataCatalog.Url.Tooltip")
  @HopMetadataProperty
  private String url;

  @GuiWidgetElement(
      id = "token",
      order = "020",
      parentId = DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::DataHubDataCatalog.Token.Label",
      toolTip = "i18n::DataHubDataCatalog.Token.Tooltip")
  @HopMetadataProperty(password = true)
  private String token;

  @GuiWidgetElement(
      id = "environment",
      order = "030",
      parentId = DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DataHubDataCatalog.Environment.Label",
      toolTip = "i18n::DataHubDataCatalog.Environment.Tooltip")
  @HopMetadataProperty
  private String environment;

  @GuiWidgetElement(
      id = "connectTimeoutMs",
      order = "040",
      parentId = DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DataHubDataCatalog.ConnectTimeout.Label",
      toolTip = "i18n::DataHubDataCatalog.ConnectTimeout.Tooltip")
  @HopMetadataProperty
  private String connectTimeoutMs;

  @GuiWidgetElement(
      id = "readTimeoutMs",
      order = "050",
      parentId = DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DataHubDataCatalog.ReadTimeout.Label",
      toolTip = "i18n::DataHubDataCatalog.ReadTimeout.Tooltip")
  @HopMetadataProperty
  private String readTimeoutMs;

  @GuiWidgetElement(
      id = "trustStorePath",
      order = "060",
      parentId = DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::DataHubDataCatalog.TrustStorePath.Label",
      toolTip = "i18n::DataHubDataCatalog.TrustStorePath.Tooltip")
  @HopMetadataProperty
  private String trustStorePath;

  @GuiWidgetElement(
      id = "trustStorePassword",
      order = "070",
      parentId = DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::DataHubDataCatalog.TrustStorePassword.Label",
      toolTip = "i18n::DataHubDataCatalog.TrustStorePassword.Tooltip")
  @HopMetadataProperty(password = true)
  private String trustStorePassword;

  @GuiWidgetElement(
      id = "trustStoreType",
      order = "080",
      parentId = DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DataHubDataCatalog.TrustStoreType.Label",
      toolTip = "i18n::DataHubDataCatalog.TrustStoreType.Tooltip")
  @HopMetadataProperty
  private String trustStoreType;

  /** The client of an initialized copy. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private DataHubClient client;

  /** The resolved environment of an initialized copy. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private String actualEnvironment;

  public DataHubDataCatalog() {
    environment = DEFAULT_ENVIRONMENT;
    connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
    readTimeoutMs = DEFAULT_READ_TIMEOUT_MS;
  }

  @Override
  public DataHubDataCatalog clone() {
    DataHubDataCatalog copy = new DataHubDataCatalog();
    copy.pluginId = pluginId;
    copy.pluginName = pluginName;
    copy.url = url;
    copy.token = token;
    copy.environment = environment;
    copy.connectTimeoutMs = connectTimeoutMs;
    copy.readTimeoutMs = readTimeoutMs;
    copy.trustStorePath = trustStorePath;
    copy.trustStorePassword = trustStorePassword;
    copy.trustStoreType = trustStoreType;
    return copy;
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    String actualUrl = variables.resolve(url);
    if (StringUtils.isBlank(actualUrl)) {
      throw new HopException("The DataHub catalog has no URL");
    }
    try {
      actualUrl = DataHubSinkConfig.normalizeUrl(actualUrl.trim());
    } catch (IllegalArgumentException e) {
      throw new HopException(e.getMessage(), e);
    }
    actualEnvironment =
        StringUtils.defaultIfBlank(variables.resolve(environment), DEFAULT_ENVIRONMENT)
            .trim()
            .toUpperCase(Locale.ROOT);
    client =
        new DataHubClient(
            actualUrl,
            StringUtils.trimToNull(secret(variables, token)),
            Duration.ofMillis(millis(variables, connectTimeoutMs, DEFAULT_CONNECT_TIMEOUT_MS)),
            Duration.ofMillis(millis(variables, readTimeoutMs, DEFAULT_READ_TIMEOUT_MS)),
            variables.resolve(trustStorePath),
            secret(variables, trustStorePassword),
            StringUtils.defaultIfBlank(variables.resolve(trustStoreType), "PKCS12"));
  }

  private static String secret(IVariables variables, String value) {
    return Encr.decryptPasswordOptionallyEncrypted(variables.resolve(value));
  }

  private static long millis(IVariables variables, String value, String defaultValue)
      throws HopException {
    String resolved = StringUtils.defaultIfBlank(variables.resolve(value), defaultValue).trim();
    try {
      long millis = Long.parseLong(resolved);
      if (millis <= 0) {
        throw new NumberFormatException();
      }
      return millis;
    } catch (NumberFormatException e) {
      throw new HopException("A DataHub timeout must be a positive number of ms, was: " + resolved);
    }
  }

  @Override
  public CatalogAnswer<String> ping() {
    CatalogAnswer<JsonNode> answer = query(PING_QUERY, MAPPER.createObjectNode());
    if (!(answer instanceof CatalogAnswer.Known<JsonNode> known)) {
      return answer.unknownAs();
    }
    String version = known.value().path("appConfig").path("appVersion").asText("");
    String user = known.value().path("me").path("corpUser").path("username").asText("");
    return CatalogAnswer.known(
        ("DataHub " + version).trim() + (user.isEmpty() ? "" : ", reading as " + user));
  }

  @Override
  public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
    if (dataset == null || !dataset.isComplete()) {
      return CatalogAnswer.unknown(
          Reason.NOT_RESOLVABLE, dataset == null ? "no dataset" : dataset.describe());
    }
    String urn =
        StringUtils.isNotBlank(dataset.catalogId())
            ? dataset.catalogId().trim()
            : DataHubUrns.datasetForOpenLineage(
                dataset.namespace(), dataset.name(), actualEnvironment);
    CatalogAnswer<JsonNode> answer = datasetQuery(EXISTS_QUERY, urn, null, 0);
    if (!(answer instanceof CatalogAnswer.Known<JsonNode> known)) {
      return answer.unknownAs();
    }
    JsonNode found = known.value();
    if (found.isMissingNode() || found.isNull() || !found.path("exists").asBoolean(true)) {
      return CatalogAnswer.notFound();
    }
    return CatalogAnswer.known(new CatalogEntityId(found.path("urn").asText(urn)));
  }

  @Override
  public CatalogAnswer<Deprecation> deprecation(CatalogEntityId id) {
    CatalogAnswer<JsonNode> answer = datasetQuery(DEPRECATION_QUERY, id.value(), null, 0);
    if (!(answer instanceof CatalogAnswer.Known<JsonNode> known)) {
      return answer.unknownAs();
    }
    JsonNode deprecation = known.value().path("deprecation");
    if (deprecation.isMissingNode() || deprecation.isNull()) {
      return CatalogAnswer.notFound();
    }
    return CatalogAnswer.known(
        new Deprecation(
            deprecation.path("deprecated").asBoolean(false),
            StringUtils.trimToNull(deprecation.path("note").asText(null)),
            instant(deprecation.path("decommissionTime"))));
  }

  @Override
  public CatalogAnswer<Instant> lastUpdated(CatalogEntityId id) {
    CatalogAnswer<JsonNode> answer =
        datasetQuery(OPERATIONS_QUERY, id.value(), "limit", MAX_OPERATIONS);
    if (!(answer instanceof CatalogAnswer.Known<JsonNode> known)) {
      return answer.unknownAs();
    }
    Instant last = null;
    for (JsonNode operation : known.value().path("operations")) {
      Instant updated = instant(operation.path("lastUpdatedTimestamp"));
      if (updated != null && (last == null || updated.isAfter(last))) {
        last = updated;
      }
    }
    return last == null ? CatalogAnswer.notFound() : CatalogAnswer.known(last);
  }

  @Override
  public CatalogAnswer<List<CatalogField>> schema(CatalogEntityId id) {
    CatalogAnswer<JsonNode> answer = datasetQuery(SCHEMA_QUERY, id.value(), null, 0);
    if (!(answer instanceof CatalogAnswer.Known<JsonNode> known)) {
      return answer.unknownAs();
    }
    JsonNode fields = known.value().path("schemaMetadata").path("fields");
    if (!fields.isArray() || fields.isEmpty()) {
      return CatalogAnswer.notFound();
    }
    // Tags and terms added in the DataHub UI live apart from the ingested schema
    // and can be stored under a v1 and a v2 path of the same field
    Map<String, List<JsonNode>> edited = new LinkedHashMap<>();
    for (JsonNode info :
        known.value().path("editableSchemaMetadata").path("editableSchemaFieldInfo")) {
      edited
          .computeIfAbsent(fieldName(info.path("fieldPath").asText("")), k -> new ArrayList<>())
          .add(info);
    }
    List<CatalogField> result = new ArrayList<>();
    for (JsonNode field : fields) {
      String name = fieldName(field.path("fieldPath").asText(""));
      if (name.isEmpty()) {
        continue;
      }
      Set<String> tags = new LinkedHashSet<>();
      Set<String> terms = new LinkedHashSet<>();
      addLabels(field, tags, terms);
      for (JsonNode editedField : edited.getOrDefault(name, List.of())) {
        addLabels(editedField, tags, terms);
      }
      result.add(
          new CatalogField(
              name,
              StringUtils.trimToNull(field.path("nativeDataType").asText(null)),
              StringUtils.trimToNull(field.path("type").asText(null)),
              field.path("nullable").isBoolean() ? field.path("nullable").asBoolean() : null,
              Set.copyOf(tags),
              Set.copyOf(terms)));
    }
    return CatalogAnswer.known(result);
  }

  /**
   * The name of a field: its path, without the type and version parts of a DataHub v2 path ({@code
   * [version=2.0].[type=string].email} is {@code email}).
   */
  static String fieldName(String fieldPath) {
    if (!fieldPath.startsWith("[")) {
      return fieldPath;
    }
    // The bracketed parts can hold dots of their own: [version=2.0]
    return StringUtils.strip(V2_PATH_PART.matcher(fieldPath).replaceAll(""), ".");
  }

  private static void addLabels(JsonNode field, Set<String> tags, Set<String> terms) {
    for (JsonNode association : field.path("globalTags").path("tags")) {
      JsonNode tag = association.path("tag");
      addNames(tags, tag.path("urn"), tag.path("name"), tag.path("properties").path("name"));
    }
    for (JsonNode association : field.path("glossaryTerms").path("terms")) {
      JsonNode term = association.path("term");
      addNames(
          terms,
          term.path("urn"),
          term.path("name"),
          term.path("hierarchicalName"),
          term.path("properties").path("name"));
      String path = glossaryPath(term);
      if (path != null) {
        terms.add(path);
      }
    }
  }

  /**
   * The path of a term in the glossary, from the top node down: {@code Classification.Personal}.
   * DataHub's hierarchicalName is the id in the term's URN, which is only a path for terms ingested
   * from a business glossary file. A term created in the DataHub UI has a generated id.
   */
  private static String glossaryPath(JsonNode term) {
    String name = StringUtils.trimToNull(term.path("properties").path("name").asText(null));
    JsonNode parents = term.path("parentNodes").path("nodes");
    if (name == null || !parents.isArray() || parents.isEmpty()) {
      return null;
    }
    // DataHub lists the direct parent first
    List<String> parts = new ArrayList<>();
    for (JsonNode parent : parents) {
      String parentName =
          StringUtils.trimToNull(parent.path("properties").path("name").asText(null));
      if (parentName == null) {
        return null;
      }
      parts.add(0, parentName);
    }
    parts.add(name);
    return String.join(".", parts);
  }

  private static void addNames(Set<String> names, JsonNode... values) {
    for (JsonNode value : values) {
      String text = StringUtils.trimToNull(value.asText(null));
      if (text != null) {
        names.add(text);
      }
    }
  }

  @Override
  public CatalogAnswer<List<AssertionResult>> latestAssertionResults(CatalogEntityId id) {
    CatalogAnswer<JsonNode> answer =
        datasetQuery(ASSERTIONS_QUERY, id.value(), "count", MAX_ASSERTIONS);
    if (!(answer instanceof CatalogAnswer.Known<JsonNode> known)) {
      return answer.unknownAs();
    }
    List<AssertionResult> results = new ArrayList<>();
    for (JsonNode assertion : known.value().path("assertions").path("assertions")) {
      JsonNode latest = assertion.path("runEvents").path("runEvents").path(0);
      results.add(
          new AssertionResult(
              assertion.path("urn").asText(),
              StringUtils.trimToNull(assertion.path("info").path("description").asText(null)),
              outcome(latest.path("result").path("type").asText("")),
              instant(latest.path("timestampMillis"))));
    }
    return results.isEmpty() ? CatalogAnswer.notFound() : CatalogAnswer.known(results);
  }

  static Outcome outcome(String resultType) {
    return switch (resultType) {
      case "SUCCESS" -> Outcome.PASSED;
      case "FAILURE" -> Outcome.FAILED;
      case "ERROR" -> Outcome.ERROR;
      default -> Outcome.UNKNOWN;
    };
  }

  private static Instant instant(JsonNode millis) {
    return millis.canConvertToLong() && millis.asLong() > 0
        ? Instant.ofEpochMilli(millis.asLong())
        : null;
  }

  /** The {@code dataset} of a query on one URN, with an optional numeric variable. */
  private CatalogAnswer<JsonNode> datasetQuery(
      String query, String urn, String numberName, int number) {
    ObjectNode variables = MAPPER.createObjectNode();
    variables.put("urn", urn);
    if (numberName != null) {
      variables.put(numberName, number);
    }
    CatalogAnswer<JsonNode> answer = query(query, variables);
    if (answer instanceof CatalogAnswer.Known<JsonNode> known) {
      // A URN DataHub doesn't have gives a null dataset: every field of it is then missing
      return CatalogAnswer.known(known.value().path("dataset"));
    }
    return answer;
  }

  private CatalogAnswer<JsonNode> query(String query, ObjectNode variables) {
    if (client == null) {
      return CatalogAnswer.unknown(Reason.ERROR, "The DataHub catalog was not initialized");
    }
    try {
      return CatalogAnswer.known(client.graphql(query, variables));
    } catch (DataHubClient.ReadException e) {
      return CatalogAnswer.unknown(reason(e.status(), e.getMessage()), e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return CatalogAnswer.unknown(Reason.ERROR, "Interrupted");
    }
  }

  /** Why DataHub could not answer, from the status of a read failure. */
  static Reason reason(int status, String message) {
    return switch (status) {
        // No answer: the client names the exception, an HttpTimeoutException for a timeout
      case -1 -> Const.NVL(message, "").contains("Timeout") ? Reason.TIMEOUT : Reason.UNREACHABLE;
      case 401, 403 -> Reason.UNAUTHORIZED;
      case 408, 504 -> Reason.TIMEOUT;
      default -> Reason.ERROR;
    };
  }

  @Override
  public void close() {
    if (client != null) {
      client.shutdown();
      client = null;
    }
  }
}
