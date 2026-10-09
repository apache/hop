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

package org.apache.hop.gremlin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphUpsertNode;
import org.apache.hop.core.graph.GraphUpsertRelationship;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.jupiter.api.Test;

/** The upsert traversals, translated to Gremlin scripts. No server needed. */
class GremlinUpsertTraversalTest {
  private final GraphTraversalSource g = EmptyGraph.instance().traversal();

  private static String script(Object traversal) {
    return GroovyTranslator.of("g")
        .translate(((Traversal<?, ?>) traversal).asAdmin().getBytecode())
        .getScript();
  }

  @Test
  void vertexPropertiesHaveSingleCardinality() {
    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put("name", "Ann");
    properties.put("nothing", null);
    String script =
        script(
            GremlinGraphConnection.upsertNodes(
                g, List.of(new GraphUpsertNode("Person", Map.of("pid", 1L), properties))));
    assertEquals(
        "g.inject(0L).sideEffect(__.V().hasLabel(\"Person\").has(\"pid\",1L).limit(1L).fold()"
            + ".coalesce(__.unfold(),__.addV(\"Person\")"
            + ".property(VertexProperty.Cardinality.single,\"pid\",1L))"
            + ".property(VertexProperty.Cardinality.single,\"name\",\"Ann\"))",
        script);
  }

  /**
   * Every node is a side effect of the one injected traverser: a key that matches two vertices
   * doesn't make the next nodes run twice.
   */
  @Test
  void everyNodeRunsOnce() {
    String script =
        script(
            GremlinGraphConnection.upsertNodes(
                g,
                List.of(
                    new GraphUpsertNode("Person", Map.of("pid", 1L), Map.of()),
                    new GraphUpsertNode("Person", Map.of("pid", 2L), Map.of()))));
    assertEquals(
        "g.inject(0L)"
            + ".sideEffect(__.V().hasLabel(\"Person\").has(\"pid\",1L).limit(1L).fold()"
            + ".coalesce(__.unfold(),__.addV(\"Person\")"
            + ".property(VertexProperty.Cardinality.single,\"pid\",1L)))"
            + ".sideEffect(__.V().hasLabel(\"Person\").has(\"pid\",2L).limit(1L).fold()"
            + ".coalesce(__.unfold(),__.addV(\"Person\")"
            + ".property(VertexProperty.Cardinality.single,\"pid\",2L)))",
        script);
  }

  /** The first existing edge or a new one, as a side effect of the one injected traverser. */
  @Test
  void everyRelationshipRunsOnce() {
    GraphUpsertNode a = new GraphUpsertNode("Person", Map.of("pid", 1L), Map.of());
    GraphUpsertNode b = new GraphUpsertNode("Person", Map.of("pid", 2L), Map.of());
    String script =
        script(
            GremlinGraphConnection.upsertRelationships(
                g,
                List.of(
                    new GraphUpsertRelationship("KNOWS", a, b, Map.of()),
                    new GraphUpsertRelationship("KNOWS", b, a, Map.of()))));
    String ab = upsertKnows("1L", "2L");
    String ba = upsertKnows("2L", "1L");
    assertEquals("g.inject(0L)" + ab + ba, script);
  }

  private static String upsertKnows(String from, String to) {
    return (".sideEffect(__.coalesce("
            + "__.V().hasLabel(\"Person\").has(\"pid\",FROM).outE(\"KNOWS\")"
            + ".where(__.inV().hasLabel(\"Person\").has(\"pid\",TO)).limit(1L),"
            + "__.addE(\"KNOWS\").from(__.V().hasLabel(\"Person\").has(\"pid\",FROM).limit(1L))"
            + ".to(__.V().hasLabel(\"Person\").has(\"pid\",TO).limit(1L))))")
        .replace("FROM", from)
        .replace("TO", to);
  }

  /**
   * The upsert scripts parse with the Gremlin grammar, on the ANTLR runtime Hop ships in lib/core.
   */
  @Test
  void upsertScriptsParse() throws Exception {
    GraphUpsertNode a = new GraphUpsertNode("Person", Map.of("pid", 1L), Map.of("name", "Ann"));
    GraphUpsertNode b = new GraphUpsertNode("Person", Map.of("pid", 2L), Map.of());
    for (Object traversal :
        List.of(
            GremlinGraphConnection.upsertNodes(g, List.of(a, b)),
            GremlinGraphConnection.upsertRelationships(
                g, List.of(new GraphUpsertRelationship("KNOWS", a, b, Map.of("since", 2020L)))))) {
      String script = script(traversal);
      // The grammar knows the cardinality as Cardinality.single, the translator writes the class
      Traversal<?, ?> parsed =
          (Traversal<?, ?>)
              GremlinQueryParser.parse(
                  script.replace("VertexProperty.Cardinality.", "Cardinality."));
      assertEquals(script, script(parsed));
    }
  }

  @Test
  void nullKeyValuesAreRejected() {
    Map<String, Object> keys = new LinkedHashMap<>();
    keys.put("pid", null);
    GraphUpsertNode nullKey = new GraphUpsertNode("Person", keys, Map.of());
    GraphUpsertNode keyed = new GraphUpsertNode("Person", Map.of("pid", 1L), Map.of());
    GremlinGraphConnection connection = new GremlinGraphConnection(null, null, g, null);
    HopException e =
        assertThrows(HopException.class, () -> connection.upsert(List.of(nullKey), List.of()));
    assertTrue(e.getMessage().contains("'Person'"), e.getMessage());
    assertTrue(e.getMessage().contains("'pid'"), e.getMessage());
    assertThrows(
        HopException.class,
        () ->
            connection.upsert(
                List.of(),
                List.of(new GraphUpsertRelationship("KNOWS", keyed, nullKey, Map.of()))));
  }

  @Test
  void edgePropertiesHaveNoCardinality() {
    GraphUpsertNode a = new GraphUpsertNode("Person", Map.of("pid", 1L), Map.of());
    GraphUpsertNode b = new GraphUpsertNode("Person", Map.of("pid", 2L), Map.of());
    String script =
        script(
            GremlinGraphConnection.upsertRelationships(
                g, List.of(new GraphUpsertRelationship("KNOWS", a, b, Map.of("since", 2020L)))));
    assertTrue(script.endsWith(".property(\"since\",2020L))"), script);
    assertFalse(script.contains("Cardinality"), script);
  }

  @Test
  void nodesWithoutKeysAreRejected() {
    GraphUpsertNode noKeys = new GraphUpsertNode("Person", Map.of(), Map.of("name", "Ann"));
    GraphUpsertNode keyed = new GraphUpsertNode("Person", Map.of("pid", 1L), Map.of());
    // Without keys V().hasLabel() would match, and update, every vertex with the label
    assertThrows(
        IllegalStateException.class, () -> GremlinGraphConnection.upsertNodes(g, List.of(noKeys)));
    assertThrows(
        IllegalStateException.class,
        () ->
            GremlinGraphConnection.upsertRelationships(
                g, List.of(new GraphUpsertRelationship("KNOWS", keyed, noKeys, Map.of()))));
    // The connection checks before sending anything
    GremlinGraphConnection connection = new GremlinGraphConnection(null, null, g, null);
    HopException e =
        assertThrows(HopException.class, () -> connection.upsert(List.of(noKeys), List.of()));
    assertTrue(e.getMessage().contains("no key properties"), e.getMessage());
    assertThrows(
        HopException.class,
        () ->
            connection.upsert(
                List.of(), List.of(new GraphUpsertRelationship("KNOWS", noKeys, keyed, Map.of()))));
  }
}
