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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.langchain4j.model.chat.request.json.JsonArraySchema;
import dev.langchain4j.model.chat.request.json.JsonEnumSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.request.json.JsonStringSchema;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class GraphExtractionTest {

  private static final String ANSWER =
      """
      {"entities": [
         {"name": "Ada Lovelace", "type": "Person", "description": "A mathematician."},
         {"name": "Analytical Engine", "type": "Machine", "description": "A computer design."},
         {"name": "Ada Lovelace", "type": "Person", "description": "A mathematician."},
         {"name": "  ", "type": "Person", "description": "no name"}
       ],
       "relationships": [
         {"source": "Ada Lovelace", "target": "Analytical Engine", "type": "WROTE_ABOUT",
          "description": "She wrote the first program for it."},
         {"source": "", "target": "Analytical Engine", "type": "WROTE_ABOUT", "description": ""}
       ]}
      """;

  @Test
  void readsEntitiesAndRelationships() throws HopException {
    GraphExtraction graph = GraphExtraction.parse(ANSWER, List.of(), List.of());

    // The duplicate and the nameless entity are left out, as is the relationship without a source
    assertEquals(2, graph.entities().size());
    assertEquals(
        new GraphExtraction.Entity("Ada Lovelace", "Person", "A mathematician."),
        graph.entities().get(0));
    assertEquals(1, graph.relationships().size());
    GraphExtraction.Relationship relationship = graph.relationships().get(0);
    assertEquals("Ada Lovelace", relationship.source());
    assertEquals("Analytical Engine", relationship.target());
    assertEquals("WROTE_ABOUT", relationship.type());
  }

  @Test
  void aFencedAnswerAndMissingListsAreFine() throws HopException {
    GraphExtraction graph =
        GraphExtraction.parse("```json\n{\"entities\": []}\n```", List.of(), List.of());
    assertTrue(graph.entities().isEmpty());
    assertTrue(graph.relationships().isEmpty());
  }

  @Test
  void aTypeOutsideTheAllowedTypesIsDroppedAndTheRestKept() throws HopException {
    GraphExtraction graph = GraphExtraction.parse(ANSWER, List.of("Person"), List.of());

    assertEquals(
        List.of(new GraphExtraction.Entity("Ada Lovelace", "Person", "A mathematician.")),
        graph.entities());
    // The relationship points at the dropped Machine, so it goes too
    assertTrue(graph.relationships().isEmpty());
    assertEquals(2, graph.dropped().size(), graph.dropped().toString());
    assertTrue(graph.dropped().get(0).contains("'Machine'"), graph.dropped().get(0));

    GraphExtraction noRelationships =
        GraphExtraction.parse(ANSWER, List.of(), List.of("WORKS_FOR"));
    assertEquals(2, noRelationships.entities().size());
    assertTrue(noRelationships.relationships().isEmpty());
    assertTrue(
        noRelationships.dropped().get(0).contains("'WROTE_ABOUT'"),
        noRelationships.dropped().get(0));
  }

  @Test
  void typesMatchIgnoringCaseAndAreWrittenAsConfigured() throws HopException {
    String answer =
        """
        {"entities": [{"name": "Ada", "type": "person", "description": ""},
                      {"name": "Engine", "type": "MACHINE", "description": ""}],
         "relationships": [{"source": "Ada", "target": "Engine", "type": "wrote_about",
                            "description": ""}]}
        """;
    GraphExtraction graph =
        GraphExtraction.parse(answer, List.of("Person", "Machine"), List.of("WROTE_ABOUT"));

    assertEquals("Person", graph.entities().get(0).type());
    assertEquals("Machine", graph.entities().get(1).type());
    assertEquals("WROTE_ABOUT", graph.relationships().get(0).type());
    assertTrue(graph.dropped().isEmpty());
  }

  @Test
  void relationshipEndsMustBeEntitiesAndTakeTheirSpelling() throws HopException {
    String answer =
        """
        {"entities": [{"name": "Ada Lovelace", "type": "Person", "description": ""},
                      {"name": "Analytical Engine", "type": "Machine", "description": ""}],
         "relationships": [
           {"source": " ada lovelace ", "target": "ANALYTICAL ENGINE", "type": "WROTE_ABOUT",
            "description": ""},
           {"source": "Ada Lovelace", "target": "Charles Babbage", "type": "WORKED_WITH",
            "description": ""}]}
        """;
    GraphExtraction graph = GraphExtraction.parse(answer, List.of(), List.of());

    assertEquals(1, graph.relationships().size());
    GraphExtraction.Relationship relationship = graph.relationships().get(0);
    assertEquals("Ada Lovelace", relationship.source());
    assertEquals("Analytical Engine", relationship.target());
    assertEquals(1, graph.dropped().size());
    assertTrue(graph.dropped().get(0).contains("'Charles Babbage'"), graph.dropped().get(0));
  }

  @Test
  void unreadableAnswersAreReported() {
    assertThrows(
        HopException.class, () -> GraphExtraction.parse("no graph here", List.of(), List.of()));
    assertThrows(
        HopException.class,
        () -> GraphExtraction.parse("{\"entities\": \"Ada\"}", List.of(), List.of()));
  }

  @Test
  void schemaConstrainsTypesOnlyWhenGiven() {
    JsonSchema free = GraphExtraction.schema(List.of(), List.of(), "Extract graph");
    assertEquals("Extract_graph", free.name());
    assertTrue(entityType(free) instanceof JsonStringSchema);

    JsonSchema constrained =
        GraphExtraction.schema(List.of("Person", "Machine"), List.of("WROTE_ABOUT"), "x");
    assertEquals(
        List.of("Person", "Machine"), ((JsonEnumSchema) entityType(constrained)).enumValues());
  }

  @Test
  void promptNamesTheTypesAndInstructions() {
    String prompt =
        GraphExtraction.systemPrompt(
            List.of("Person"), List.of("WORKS_FOR"), "The texts are about history.");
    assertTrue(prompt.contains("Entity types, use only these: Person"), prompt);
    assertTrue(prompt.contains("Relationship types, use only these: WORKS_FOR"), prompt);
    assertTrue(prompt.contains("about history"), prompt);
    assertTrue(GraphExtraction.systemPrompt(List.of(), List.of(), "").contains("UPPER_SNAKE_CASE"));
  }

  private static Object entityType(JsonSchema schema) {
    JsonObjectSchema root = (JsonObjectSchema) schema.rootElement();
    JsonArraySchema entities = (JsonArraySchema) root.properties().get("entities");
    return ((JsonObjectSchema) entities.items()).properties().get("type");
  }
}
