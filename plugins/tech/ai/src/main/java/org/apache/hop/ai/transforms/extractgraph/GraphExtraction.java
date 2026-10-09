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

package org.apache.hop.ai.transforms.extractgraph;

import com.fasterxml.jackson.databind.JsonNode;
import dev.langchain4j.model.chat.request.json.JsonArraySchema;
import dev.langchain4j.model.chat.request.json.JsonEnumSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.request.json.JsonSchemaElement;
import dev.langchain4j.model.chat.request.json.JsonStringSchema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hop.ai.transforms.structuredextract.ExtractionParser;
import org.apache.hop.core.exception.HopException;

/**
 * The entities and relationships the model read from one text, and the JSON schema that asks for
 * them.
 *
 * <p>One call to the model reads a whole text, so one stray element does not fail the row: an
 * entity or relationship whose type is outside the configured types, or a relationship between
 * names that aren't among the entities, is left out and reported in {@link #dropped()}. Elements
 * without a name, source or target carry nothing to put in a graph and are left out silently, as
 * are exact duplicates.
 */
public final class GraphExtraction {

  /** An entity: a thing the text mentions. */
  public record Entity(String name, String type, String description) {}

  /** A relationship between two entities, by their names. */
  public record Relationship(String source, String target, String type, String description) {}

  private final List<Entity> entities;
  private final List<Relationship> relationships;
  private final List<String> dropped;

  public GraphExtraction(List<Entity> entities, List<Relationship> relationships) {
    this(entities, relationships, List.of());
  }

  public GraphExtraction(
      List<Entity> entities, List<Relationship> relationships, List<String> dropped) {
    this.entities = entities;
    this.relationships = relationships;
    this.dropped = dropped;
  }

  /** What the answer held that was left out, and why: one line per element. */
  public List<String> dropped() {
    return dropped;
  }

  public List<Entity> entities() {
    return entities;
  }

  public List<Relationship> relationships() {
    return relationships;
  }

  /**
   * The schema of the answer: two arrays, constrained to the given types when there are any.
   *
   * @param entityTypes the only entity types allowed, empty for any
   * @param relationshipTypes the only relationship types allowed, empty for any
   */
  public static JsonSchema schema(
      List<String> entityTypes, List<String> relationshipTypes, String schemaName) {
    JsonObjectSchema entity =
        JsonObjectSchema.builder()
            .addProperty(
                "name", JsonStringSchema.builder().description("as the text names it").build())
            .addProperty("type", typeElement(entityTypes, "what kind of thing it is"))
            .addProperty(
                "description", JsonStringSchema.builder().description("one sentence").build())
            .required("name", "type", "description")
            .additionalProperties(false)
            .build();
    JsonObjectSchema relationship =
        JsonObjectSchema.builder()
            .addProperty("source", JsonStringSchema.builder().description("an entity name").build())
            .addProperty("target", JsonStringSchema.builder().description("an entity name").build())
            .addProperty("type", typeElement(relationshipTypes, "in UPPER_SNAKE_CASE"))
            .addProperty(
                "description", JsonStringSchema.builder().description("one sentence").build())
            .required("source", "target", "type", "description")
            .additionalProperties(false)
            .build();
    return JsonSchema.builder()
        .name(
            schemaName == null || schemaName.isBlank()
                ? "graph"
                : schemaName.replaceAll("[^A-Za-z0-9_-]+", "_"))
        .rootElement(
            JsonObjectSchema.builder()
                .addProperty("entities", JsonArraySchema.builder().items(entity).build())
                .addProperty("relationships", JsonArraySchema.builder().items(relationship).build())
                .required("entities", "relationships")
                .additionalProperties(false)
                .build())
        .build();
  }

  private static JsonSchemaElement typeElement(List<String> allowed, String description) {
    if (allowed.isEmpty()) {
      return JsonStringSchema.builder().description(description).build();
    }
    return JsonEnumSchema.builder().enumValues(allowed).build();
  }

  /**
   * The instructions for the model, with the schema spelled out for models that can't be held to
   * it.
   */
  public static String systemPrompt(
      List<String> entityTypes, List<String> relationshipTypes, String instructions) {
    StringBuilder prompt = new StringBuilder();
    prompt
        .append("Read the knowledge graph in the text the user sends: the entities it mentions ")
        .append("and the relationships between them. ")
        .append("Answer with a single JSON object and nothing else, with two arrays:\n")
        .append("- \"entities\": objects with \"name\", \"type\" and \"description\"\n")
        .append("- \"relationships\": objects with \"source\", \"target\", \"type\" and ")
        .append("\"description\"\n\n")
        .append("Name each entity as the text names it, and use the same name every time. ")
        .append("The source and target of a relationship are names from the entities. ")
        .append("Only extract what the text states. Do not invent entities or relationships. ")
        .append("Keep each description to one sentence.");
    if (!entityTypes.isEmpty()) {
      prompt.append("\n\nEntity types, use only these: ").append(String.join(", ", entityTypes));
    }
    if (!relationshipTypes.isEmpty()) {
      prompt
          .append("\nRelationship types, use only these: ")
          .append(String.join(", ", relationshipTypes));
    } else {
      prompt.append("\nWrite relationship types in UPPER_SNAKE_CASE, for example WORKS_FOR.");
    }
    if (instructions != null && !instructions.isBlank()) {
      prompt.append("\n\n").append(instructions);
    }
    return prompt.toString();
  }

  /**
   * Reads the model's answer.
   *
   * <p>A type is matched to the configured types ignoring case and written as configured. An
   * element whose type still isn't allowed, and a relationship whose source or target isn't one of
   * the entities, is dropped and named in {@link #dropped()}, so one stray element doesn't cost the
   * other elements of the text. Relationship ends are written as the entity is named.
   *
   * @throws HopException when the answer is not readable JSON
   */
  public static GraphExtraction parse(
      String answer, List<String> entityTypes, List<String> relationshipTypes) throws HopException {
    JsonNode root = ExtractionParser.readTree(answer);
    List<String> dropped = new ArrayList<>();

    Set<Entity> entities = new LinkedHashSet<>();
    Map<String, String> entityNames = new HashMap<>();
    for (JsonNode node : array(root, "entities")) {
      String name = text(node, "name");
      if (name.isEmpty()) {
        continue;
      }
      String type = allowedType(text(node, "type"), entityTypes);
      if (type == null) {
        dropped.add(
            "entity '"
                + name
                + "': type '"
                + text(node, "type")
                + "' is not one of "
                + String.join(", ", entityTypes));
        continue;
      }
      entities.add(new Entity(name, type, text(node, "description")));
      entityNames.putIfAbsent(key(name), name);
    }

    Set<Relationship> relationships = new LinkedHashSet<>();
    for (JsonNode node : array(root, "relationships")) {
      String source = text(node, "source");
      String target = text(node, "target");
      if (source.isEmpty() || target.isEmpty()) {
        continue;
      }
      String element = "relationship '" + source + " -> " + target + "'";
      String type = allowedType(text(node, "type"), relationshipTypes);
      if (type == null) {
        dropped.add(
            element
                + ": type '"
                + text(node, "type")
                + "' is not one of "
                + String.join(", ", relationshipTypes));
        continue;
      }
      String sourceName = entityNames.get(key(source));
      String targetName = entityNames.get(key(target));
      if (sourceName == null || targetName == null) {
        dropped.add(
            element
                + ": '"
                + (sourceName == null ? source : target)
                + "' is not one of the entities");
        continue;
      }
      relationships.add(new Relationship(sourceName, targetName, type, text(node, "description")));
    }
    return new GraphExtraction(new ArrayList<>(entities), new ArrayList<>(relationships), dropped);
  }

  /**
   * The type as configured, matched ignoring case, or null when it isn't allowed. Any type is
   * allowed when none are configured.
   */
  static String allowedType(String type, List<String> allowed) {
    if (allowed.isEmpty()) {
      return type;
    }
    for (String candidate : allowed) {
      if (candidate.equalsIgnoreCase(type)) {
        return candidate;
      }
    }
    return null;
  }

  private static String key(String name) {
    return name.trim().toLowerCase(Locale.ROOT);
  }

  private static Iterable<JsonNode> array(JsonNode root, String name) throws HopException {
    JsonNode node = root.get(name);
    if (node == null || node.isNull()) {
      return List.of();
    }
    if (!node.isArray()) {
      throw new HopException("Expected \"" + name + "\" to be a list in the model's answer");
    }
    return node;
  }

  private static String text(JsonNode node, String name) {
    JsonNode value = node == null ? null : node.get(name);
    return value == null || value.isNull() ? "" : value.asText().trim();
  }
}
