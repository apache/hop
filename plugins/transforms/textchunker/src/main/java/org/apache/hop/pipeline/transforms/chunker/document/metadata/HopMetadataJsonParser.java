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
package org.apache.hop.pipeline.transforms.chunker.document.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.pipeline.transforms.chunker.document.ContentType;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentNode;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentParser;
import org.apache.hop.pipeline.transforms.chunker.document.SecretRedaction;

/**
 * Parses Hop project metadata JSON into one section per connection, engine, or top-level config
 * block.
 */
public final class HopMetadataJsonParser implements DocumentParser {

  @Override
  public ContentType getContentType() {
    return ContentType.METADATA;
  }

  @Override
  public DocumentNode parse(String text) {
    if (text == null || text.isBlank()) {
      return DocumentNode.root("");
    }

    JsonNode root = parseRoot(text);
    if (root == null || !root.isObject()) {
      return DocumentNode.root(text);
    }

    List<DocumentNode> children = new ArrayList<>();
    String name = textValue(root.get("name"));
    String description = textValue(root.get("description"));

    StringBuilder overview = new StringBuilder();
    if (!name.isEmpty()) {
      overview.append("Hop metadata '").append(name).append("'\n");
    }
    if (!description.isEmpty()) {
      overview.append("Description: ").append(description).append('\n');
    }
    if (overview.length() > 0) {
      children.add(DocumentNode.leaf("Overview", overview.toString().strip(), 0));
    }

    Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String key = entry.getKey();
      if ("name".equals(key) || "description".equals(key)) {
        continue;
      }
      addSectionsForKey(children, key, entry.getValue());
    }

    if (children.isEmpty()) {
      return DocumentNode.root(text);
    }
    return new DocumentNode("", "", 0, children);
  }

  private static void addSectionsForKey(List<DocumentNode> children, String key, JsonNode value) {
    if (value == null || value.isNull()) {
      return;
    }
    if ("rdbms".equals(key) && value.isObject()) {
      value
          .fields()
          .forEachRemaining(
              conn ->
                  children.add(
                      DocumentNode.leaf(
                          "Connection: " + conn.getKey(), formatConnection(conn.getValue()), 0)));
      return;
    }
    if ("engineRunConfiguration".equals(key) && value.isObject()) {
      value
          .fields()
          .forEachRemaining(
              engine ->
                  children.add(
                      DocumentNode.leaf(
                          "Engine: " + engine.getKey(), prettyJson(engine.getValue()), 0)));
      return;
    }
    if (value.isObject() && value.size() > 1 && allValuesAreObjects(value)) {
      value
          .fields()
          .forEachRemaining(
              child ->
                  children.add(
                      DocumentNode.leaf(
                          humanizeKey(key) + ": " + child.getKey(),
                          prettyJson(child.getValue()),
                          0)));
      return;
    }
    JsonNode body = value;
    // A top-level value has no parent object for redactSecrets to check its name against.
    if (value.isValueNode()
        && !value.isBoolean()
        && SecretRedaction.REDACTED.equals(SecretRedaction.redact(key, value.asText()))) {
      body = TextNode.valueOf(SecretRedaction.REDACTED);
    }
    children.add(DocumentNode.leaf(humanizeKey(key), prettyJson(body), 0));
  }

  private static boolean allValuesAreObjects(JsonNode object) {
    Iterator<JsonNode> values = object.elements();
    while (values.hasNext()) {
      if (!values.next().isObject()) {
        return false;
      }
    }
    return true;
  }

  private static String formatConnection(JsonNode conn) {
    if (conn == null || !conn.isObject()) {
      return prettyJson(conn);
    }
    StringBuilder body = new StringBuilder();
    appendLine(body, "Plugin", textValue(conn.get("pluginName")));
    appendLine(body, "Host", textValue(conn.get("hostname")));
    appendLine(body, "Port", textValue(conn.get("port")));
    appendLine(body, "Database", textValue(conn.get("databaseName")));
    appendLine(body, "Username", textValue(conn.get("username")));
    appendLine(body, "Manual URL", textValue(conn.get("manualUrl")));
    return body.toString().strip();
  }

  private static void appendLine(StringBuilder body, String label, String value) {
    if (!value.isEmpty()) {
      body.append(label).append(": ").append(value).append('\n');
    }
  }

  private static String humanizeKey(String key) {
    if (key == null || key.isEmpty()) {
      return "Configuration";
    }
    String spaced = key.replaceAll("([a-z])([A-Z])", "$1 $2").replace('_', ' ');
    return spaced.substring(0, 1).toUpperCase() + spaced.substring(1);
  }

  private static String textValue(JsonNode node) {
    if (node == null || node.isNull()) {
      return "";
    }
    if (node.isTextual()) {
      return node.asText().strip();
    }
    return node.asText("").strip();
  }

  private static String prettyJson(JsonNode node) {
    if (node == null || node.isNull()) {
      return "";
    }
    try {
      return HopJson.newMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(redactSecrets(node.deepCopy()));
    } catch (Exception e) {
      return node.toString();
    }
  }

  /** Replace credential-bearing values before the node is turned into chunk text. */
  private static JsonNode redactSecrets(JsonNode node) {
    if (node instanceof ObjectNode object) {
      Iterator<String> names = object.fieldNames();
      List<String> fields = new ArrayList<>();
      while (names.hasNext()) {
        fields.add(names.next());
      }
      boolean secretEntry = isSecretEntry(object, fields);
      for (String field : fields) {
        JsonNode value = object.get(field);
        // null and true/false say whether something is set, not what it is.
        if (value == null || value.isNull() || value.isBoolean()) {
          continue;
        }
        boolean entryValue = secretEntry && SecretRedaction.isEntryValueKey(field);
        if (value.isValueNode()) {
          String text = value.asText();
          String redacted =
              entryValue ? SecretRedaction.redactValue(text) : SecretRedaction.redact(field, text);
          if (SecretRedaction.REDACTED.equals(redacted)) {
            object.put(field, SecretRedaction.REDACTED);
          }
        } else if (entryValue && !value.isEmpty()) {
          // A list or object under a secret entry has no names of its own to check.
          object.put(field, SecretRedaction.REDACTED);
        } else {
          redactSecrets(value);
        }
      }
    } else if (node != null && node.isArray()) {
      node.forEach(HopMetadataJsonParser::redactSecrets);
    }
    return node;
  }

  /**
   * True for a name/value entry whose name reads like a credential, such as {@code {"name":
   * "API_TOKEN", "value": "..."}} in a project, environment or run configuration, {@code {"name":
   * "Authorization", "value": "..."}} in a REST connection's headers, or {@code {"key":
   * "DB_PASSWORD", "value": "..."}} in a pipeline unit test. The value sits under {@code value}, so
   * checking field names alone would keep it.
   */
  private static boolean isSecretEntry(ObjectNode object, List<String> fields) {
    for (String field : fields) {
      JsonNode value = object.get(field);
      if (("name".equals(field) || "key".equals(field))
          && value != null
          && value.isTextual()
          && SecretRedaction.isSecretEntryName(value.asText())) {
        return true;
      }
    }
    return false;
  }

  static JsonNode parseRoot(String text) {
    String trimmed = text.stripLeading();
    if (trimmed.startsWith("{")) {
      try {
        return JsonUtil.parse(text);
      } catch (Exception ignored) {
        return null;
      }
    }
    int marker = text.indexOf("Configuration:");
    if (marker >= 0) {
      String jsonPart = text.substring(marker + "Configuration:".length()).strip();
      if (jsonPart.startsWith("{")) {
        try {
          return JsonUtil.parse(jsonPart);
        } catch (Exception ignored) {
          return null;
        }
      }
    }
    return null;
  }
}
