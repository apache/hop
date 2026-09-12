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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;

/** Validates and saves CLIPBOARD_METADATA / SAVE_METADATA hop_proposals. */
public final class AiMetadataProposalSupport {

  public static final int MAX_JSON_CHARS = 100_000;

  private AiMetadataProposalSupport() {}

  public static AiProposalValidation validate(
      AiProposal proposal, IHopMetadataProvider metadataProvider) {
    AiProposalValidation result = new AiProposalValidation();
    result.setProposalId(proposal != null ? proposal.getId() : null);
    String error = validateError(proposal, metadataProvider);
    if (error != null) {
      result.setBlocked(true);
      result.setReason(error);
      return result;
    }
    if (AiProposalXmlSupport.containsSecrets(jsonParam(proposal))) {
      result.setWarning("JSON contains password-like fields");
    } else if (AiProposalTypes.of(proposal) == AiProposalTypes.CLIPBOARD_METADATA) {
      result.setWarning("Copies JSON to the clipboard");
    } else {
      String name = Const.NVL(proposal.parameter("name"), "");
      String typeKey = Const.NVL(proposal.parameter("typeKey"), "");
      result.setWarning(
          "Saves metadata object " + (name.isEmpty() ? typeKey : name + " (" + typeKey + ")"));
    }
    return result;
  }

  public static void save(AiProposal proposal, IHopMetadataProvider metadataProvider)
      throws Exception {
    String error = validateError(proposal, metadataProvider);
    if (error != null) {
      throw new HopException(error);
    }
    String typeKey = firstParameter(proposal, "typeKey", "metadataType");
    String name = firstParameter(proposal, "name");
    Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(typeKey);
    IHopMetadata object = parseObject(metadataClass, metadataProvider, jsonParam(proposal));
    if (!Utils.isEmpty(name)) {
      object.setName(name);
    }
    if (Utils.isEmpty(object.getName())) {
      throw new HopException("Metadata name is required");
    }
    IHopMetadataSerializer<IHopMetadata> serializer = metadataProvider.getSerializer(metadataClass);
    serializer.save(object);
  }

  public static int saveAll(List<AiProposal> selected, IHopMetadataProvider provider)
      throws Exception {
    int saved = 0;
    if (selected == null || provider == null) {
      return 0;
    }
    for (AiProposal proposal : selected) {
      if (AiProposalTypes.of(proposal) == AiProposalTypes.SAVE_METADATA) {
        save(proposal, provider);
        saved++;
      }
    }
    return saved;
  }

  static String validateError(AiProposal proposal, IHopMetadataProvider metadataProvider) {
    if (proposal == null) {
      return "Missing proposal";
    }
    String typeKey = firstParameter(proposal, "typeKey", "metadataType");
    String json = jsonParam(proposal);
    if (Utils.isEmpty(typeKey)) {
      return "typeKey is required";
    }
    if (Utils.isEmpty(json)) {
      if ("rdbms".equalsIgnoreCase(typeKey)) {
        return "json parameter is required (or pluginId, hostname, databaseName). "
            + "Example json: {\"name\":\"test_edw\",\"rdbms\":{\"POSTGRESQL\":{\"pluginId\":\"POSTGRESQL\","
            + "\"hostname\":\"localhost\",\"port\":\"5432\",\"databaseName\":\"test_edw\","
            + "\"username\":\"test\",\"password\":\"${DB_PASSWORD}\"}}}";
      }
      return "json parameter is required";
    }
    if (json.length() > MAX_JSON_CHARS) {
      return "json exceeds " + MAX_JSON_CHARS + " characters";
    }
    if (metadataProvider == null) {
      return "No metadata provider";
    }
    try {
      Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(typeKey);
      parseObject(metadataClass, metadataProvider, json);
    } catch (Exception e) {
      return "Invalid metadata JSON: "
          + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
    }
    return null;
  }

  public static String jsonParam(AiProposal proposal) {
    if (proposal == null) {
      return "";
    }
    String json = firstParameter(proposal, "json", "content", "connectionJson", "rdbmsJson");
    if (Utils.isEmpty(json) || "null".equalsIgnoreCase(json) || "{}".equals(json)) {
      json = Const.NVL(synthesizeRdbmsJson(proposal), "");
    }
    String typeKey = firstParameter(proposal, "typeKey", "metadataType");
    if ("rdbms".equalsIgnoreCase(typeKey)) {
      json = normalizeRdbmsJson(json, proposal);
    }
    return json;
  }

  /**
   * Hop stores {@code IDatabase} as {@code rdbms.{PLUGIN_ID}.{fields}}. Models often emit a flat
   * object ({@code hostname} / {@code pluginId} at the top or inside {@code rdbms}). That parses as
   * DatabaseMeta defaults (type NONE). Rewrite to the wrapped shape before save.
   */
  static String normalizeRdbmsJson(String json, AiProposal proposal) {
    if (Utils.isEmpty(json)) {
      return Const.NVL(synthesizeRdbmsJson(proposal), "");
    }
    try {
      ObjectMapper mapper = HopJson.newMapper();
      JsonNode root = mapper.readTree(json);
      if (root != null && root.has("content")) {
        root = root.get("content");
      }
      if (root == null || !root.isObject()) {
        return json;
      }
      String name = firstNonEmpty(text(root, "name"), firstParameter(proposal, "name"));
      JsonNode rdbms = root.get("rdbms");
      String pluginId = resolveDatabasePluginId(proposal);
      ObjectNode inner = mapper.createObjectNode();
      if (rdbms != null && rdbms.isTextual()) {
        if (Utils.isEmpty(pluginId)) {
          pluginId = resolveDatabasePluginId(rdbms.asText());
        }
      } else if (rdbms != null && rdbms.isObject()) {
        String wrappedId = wrappedPluginKey(rdbms);
        if (!Utils.isEmpty(wrappedId)) {
          if (Utils.isEmpty(pluginId)) {
            pluginId = wrappedId;
          }
          JsonNode wrapped = rdbms.get(wrappedId);
          if (wrapped != null && wrapped.isObject()) {
            inner = (ObjectNode) wrapped.deepCopy();
          }
        } else {
          if (Utils.isEmpty(pluginId)) {
            pluginId =
                resolveDatabasePluginId(
                    text(rdbms, "pluginId", "type", "databaseType", "databasePluginId"));
          }
          inner = (ObjectNode) rdbms.deepCopy();
        }
      }
      copyIfMissing(inner, root, "hostname", "host");
      copyIfMissing(inner, root, "port");
      copyIfMissing(inner, root, "databaseName", "database");
      copyIfMissing(inner, root, "username", "user");
      copyIfMissing(inner, root, "password");
      if (Utils.isEmpty(pluginId)) {
        pluginId =
            resolveDatabasePluginId(
                text(root, "pluginId", "type", "databaseType", "databasePluginId"));
      }
      if (Utils.isEmpty(pluginId)) {
        pluginId =
            resolveDatabasePluginId(
                firstParameter(proposal, "pluginId", "databaseType", "databasePluginId", "type"));
      }
      if (Utils.isEmpty(pluginId)) {
        return json;
      }
      pluginId = resolveDatabasePluginId(pluginId);
      inner.put("pluginId", pluginId);
      if (!inner.has("pluginName") || Utils.isEmpty(text(inner, "pluginName"))) {
        String resolvedPluginName = resolveDatabasePluginName(pluginId);
        if (!Utils.isEmpty(resolvedPluginName)) {
          inner.put("pluginName", resolvedPluginName);
        }
      }
      if (!inner.has("accessType")) {
        inner.put("accessType", 0);
      }
      if (Utils.isEmpty(text(inner, "hostname"))) {
        putIfPresent(inner, "hostname", firstParameter(proposal, "hostname", "host"));
      }
      if (Utils.isEmpty(text(inner, "port"))) {
        putIfPresent(inner, "port", firstParameter(proposal, "port"));
      }
      if (Utils.isEmpty(text(inner, "databaseName"))) {
        putIfPresent(inner, "databaseName", firstParameter(proposal, "databaseName", "database"));
      }
      if (Utils.isEmpty(text(inner, "username"))) {
        putIfPresent(inner, "username", firstParameter(proposal, "username", "user"));
      }
      if (Utils.isEmpty(text(inner, "password"))) {
        putIfPresent(inner, "password", firstParameter(proposal, "password"));
      }
      ObjectNode out = mapper.createObjectNode();
      if (!Utils.isEmpty(name)) {
        out.put("name", name);
      }
      ObjectNode rdbmsOut = mapper.createObjectNode();
      rdbmsOut.set(pluginId, inner);
      out.set("rdbms", rdbmsOut);
      return mapper.writeValueAsString(out);
    } catch (Exception e) {
      return json;
    }
  }

  private static final Set<String> RDBMS_FIELD_NAMES =
      Set.of(
          "pluginid",
          "pluginname",
          "hostname",
          "host",
          "port",
          "databasename",
          "database",
          "username",
          "user",
          "password",
          "accesstype",
          "attributes",
          "type",
          "databasetype",
          "databasepluginid",
          "servername",
          "manualurl",
          "name");

  static String wrappedPluginKey(JsonNode rdbms) {
    if (rdbms == null || !rdbms.isObject()) {
      return "";
    }
    Iterator<Map.Entry<String, JsonNode>> fields = rdbms.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      if (entry.getValue() != null
          && entry.getValue().isObject()
          && !RDBMS_FIELD_NAMES.contains(entry.getKey().toLowerCase())) {
        return resolveDatabasePluginId(entry.getKey());
      }
    }
    return "";
  }

  static void copyIfMissing(ObjectNode inner, JsonNode root, String... names) {
    if (inner == null || root == null || names == null || names.length == 0) {
      return;
    }
    String target = names[0];
    if (!Utils.isEmpty(text(inner, target))) {
      return;
    }
    String value = text(root, names);
    if (!Utils.isEmpty(value)) {
      inner.put(target, value);
    }
  }

  static void putIfPresent(ObjectNode inner, String field, String value) {
    if (inner != null && !Utils.isEmpty(value)) {
      inner.put(field, value);
    }
  }

  static String text(JsonNode node, String... names) {
    if (node == null || names == null) {
      return "";
    }
    for (String name : names) {
      JsonNode value = node.get(name);
      if (value != null && value.isValueNode() && !value.isNull()) {
        String text = value.asText("");
        if (!Utils.isEmpty(text)) {
          return text.trim();
        }
      }
    }
    return "";
  }

  static String firstNonEmpty(String... values) {
    if (values == null) {
      return "";
    }
    for (String value : values) {
      if (!Utils.isEmpty(value)) {
        return value;
      }
    }
    return "";
  }

  /**
   * When the model lists connection fields next to {@code typeKey=rdbms} instead of a {@code json}
   * object, build the DatabaseMeta JSON Hop expects.
   */
  static String synthesizeRdbmsJson(AiProposal proposal) {
    if (proposal == null
        || !"rdbms".equalsIgnoreCase(firstParameter(proposal, "typeKey", "metadataType"))) {
      return "";
    }
    String pluginId = resolveDatabasePluginId(proposal);
    String hostname = firstParameter(proposal, "hostname", "host", "server", "serverName");
    String port = firstParameter(proposal, "port");
    String databaseName =
        firstParameter(proposal, "databaseName", "database", "db", "dbName", "dbname");
    String name = firstParameter(proposal, "name", "databaseName");
    String username = firstParameter(proposal, "username", "user", "userName");
    String password = firstParameter(proposal, "password");
    String blob = harvestText(proposal);
    if (Utils.isEmpty(pluginId)) {
      pluginId = resolveDatabasePluginId(blob);
    }
    HostPort hostPort = parseHostPort(firstNonEmpty(hostname, blob));
    if (Utils.isEmpty(hostname) && !Utils.isEmpty(hostPort.host())) {
      hostname = hostPort.host();
    }
    if (Utils.isEmpty(port) && !Utils.isEmpty(hostPort.port())) {
      port = hostPort.port();
    }
    if (!Utils.isEmpty(hostname) && hostname.contains(":")) {
      HostPort split = parseHostPort(hostname);
      hostname = split.host();
      if (Utils.isEmpty(port)) {
        port = split.port();
      }
    }
    if (Utils.isEmpty(databaseName)) {
      databaseName = name;
    }
    if (Utils.isEmpty(pluginId) || Utils.isEmpty(hostname) || Utils.isEmpty(databaseName)) {
      return "";
    }
    if (Utils.isEmpty(name)) {
      name = databaseName;
    }
    if (Utils.isEmpty(password)) {
      password = "${DB_PASSWORD}";
    }
    if (Utils.isEmpty(port)) {
      port = defaultPort(pluginId);
    }
    String pluginName = resolveDatabasePluginName(pluginId);
    return "{\"name\":\""
        + escapeJson(name)
        + "\",\"rdbms\":{\""
        + escapeJson(pluginId)
        + "\":{\"pluginId\":\""
        + escapeJson(pluginId)
        + (!Utils.isEmpty(pluginName) ? "\",\"pluginName\":\"" + escapeJson(pluginName) : "")
        + "\",\"accessType\":0,\"hostname\":\""
        + escapeJson(hostname)
        + "\",\"port\":\""
        + escapeJson(port)
        + "\",\"databaseName\":\""
        + escapeJson(databaseName)
        + "\",\"username\":\""
        + escapeJson(username)
        + "\",\"password\":\""
        + escapeJson(password)
        + "\"}}}";
  }

  static String harvestText(AiProposal proposal) {
    if (proposal == null) {
      return "";
    }
    StringBuilder text = new StringBuilder();
    if (!Utils.isEmpty(proposal.getDescription())) {
      text.append(proposal.getDescription()).append('\n');
    }
    if (proposal.getParameters() != null) {
      proposal.getParameters().forEach((key, value) -> text.append(value).append('\n'));
    }
    return text.toString();
  }

  record HostPort(String host, String port) {}

  static HostPort parseHostPort(String value) {
    if (Utils.isEmpty(value)) {
      return new HostPort("", "");
    }
    Matcher matcher = Pattern.compile("(?i)\\b([a-z0-9._-]+):(\\d{2,5})\\b").matcher(value);
    if (matcher.find()) {
      return new HostPort(matcher.group(1), matcher.group(2));
    }
    return new HostPort("", "");
  }

  static String defaultPort(String pluginId) {
    if ("POSTGRESQL".equals(pluginId)) {
      return "5432";
    }
    if ("MYSQL".equals(pluginId) || "MARIADB".equals(pluginId)) {
      return "3306";
    }
    if ("ORACLE".equals(pluginId)) {
      return "1521";
    }
    if ("MSSQL".equals(pluginId) || "MSSQLNATIVE".equals(pluginId)) {
      return "1433";
    }
    return "5432";
  }

  static String resolveDatabasePluginId(AiProposal proposal) {
    return resolveDatabasePluginId(
        firstParameter(
            proposal, "pluginId", "databaseType", "databasePluginId", "type", "engine", "dialect"));
  }

  static String resolveDatabasePluginId(String raw) {
    if (Utils.isEmpty(raw)) {
      return "";
    }
    String normalized = raw.trim();
    String upper = normalized.toUpperCase().replace(' ', '_').replace('-', '_');
    if (upper.contains("POSTGRES")) {
      return "POSTGRESQL";
    }
    if (upper.contains("MARIA")) {
      return "MARIADB";
    }
    if (upper.contains("MSSQL") || upper.contains("SQL_SERVER") || upper.contains("SQLSERVER")) {
      return upper.contains("NATIVE") ? "MSSQLNATIVE" : "MSSQL";
    }
    if (upper.contains("SNOWFLAKE")) {
      return "SNOWFLAKE";
    }
    if (upper.contains("MYSQL")) {
      return "MYSQL";
    }
    if (upper.contains("ORACLE")) {
      return "ORACLE";
    }
    if (upper.contains("NONE") || upper.contains("GENERIC")) {
      return "";
    }
    if (normalized.length() > 40 || normalized.contains(" ") || normalized.contains("\n")) {
      return "";
    }
    return normalized;
  }

  static String resolveDatabasePluginName(String pluginId) {
    if (Utils.isEmpty(pluginId)) {
      return "";
    }
    try {
      IPlugin plugin =
          PluginRegistry.getInstance().findPluginWithId(DatabasePluginType.class, pluginId);
      if (plugin != null && !Utils.isEmpty(plugin.getName())) {
        return plugin.getName();
      }
    } catch (Exception e) {
      // Registry might not be fully initialized in some unit test contexts
    }
    String upper = pluginId.trim().toUpperCase();
    if ("POSTGRESQL".equals(upper)) {
      return "PostgreSQL";
    }
    if ("MYSQL".equals(upper)) {
      return "MySQL";
    }
    if ("MARIADB".equals(upper)) {
      return "MariaDB";
    }
    if ("ORACLE".equals(upper)) {
      return "Oracle";
    }
    if ("SNOWFLAKE".equals(upper)) {
      return "Snowflake";
    }
    if ("MSSQL".equals(upper)) {
      return "MS SQL Server";
    }
    if ("MSSQLNATIVE".equals(upper)) {
      return "MS SQL Server (Native)";
    }
    return "";
  }

  static String firstParameter(AiProposal proposal, String... names) {
    if (proposal == null || names == null || proposal.getParameters() == null) {
      return "";
    }
    for (String name : names) {
      for (Map.Entry<String, String> entry : proposal.getParameters().entrySet()) {
        if (entry.getKey() != null
            && entry.getKey().equalsIgnoreCase(name)
            && !Utils.isEmpty(entry.getValue())) {
          return entry.getValue().trim();
        }
      }
    }
    return "";
  }

  static String escapeJson(String value) {
    if (value == null) {
      return "";
    }
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  static IHopMetadata parseObject(
      Class<IHopMetadata> metadataClass, IHopMetadataProvider metadataProvider, String json)
      throws Exception {
    ObjectMapper mapper = HopJson.newMapper();
    JsonNode root = mapper.readTree(json);
    if (root != null && root.has("content")) {
      root = root.get("content");
    }
    JsonParser jsonParser = mapper.treeAsTokens(root);
    if (jsonParser.currentToken() == null) {
      jsonParser.nextToken();
    }
    JsonMetadataParser<IHopMetadata> parser =
        new JsonMetadataParser<>(metadataClass, metadataProvider);
    IHopMetadata object = parser.loadJsonObject(metadataClass, jsonParser);
    if (object instanceof DatabaseMeta databaseMeta) {
      String pluginId = databaseMeta.getPluginId();
      if (Utils.isEmpty(pluginId) || "NONE".equalsIgnoreCase(pluginId)) {
        throw new HopException(
            "rdbms JSON is missing database type. Wrap as rdbms.POSTGRESQL (or MYSQL, …) with pluginId, hostname, port, databaseName, username, password.");
      }
    }
    return object;
  }
}
