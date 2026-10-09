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

package org.apache.hop.age;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.junit.jupiter.api.Test;

class AgeCypherTest {

  @Test
  void testReturnColumns() throws Exception {
    assertEquals(List.of(), AgeCypher.getReturnColumns("CREATE (n:Person {id: $id})"));
    assertEquals(
        List.of("id", "name"),
        AgeCypher.getReturnColumns("MATCH (n:Person) RETURN n.id AS id, n.name AS name"));
    assertEquals(
        List.of("n", "count(*)"),
        AgeCypher.getReturnColumns("MATCH (n) return n, count(*) ORDER BY n.id LIMIT 3"));
    assertEquals(
        List.of("my name"),
        AgeCypher.getReturnColumns("MATCH (n) RETURN DISTINCT n.name AS `my name`;"));
    // Commas and keywords in maps, lists, strings and subqueries don't count
    assertEquals(
        List.of("m", "l"),
        AgeCypher.getReturnColumns(
            "MATCH (n) WHERE n.x = 'RETURN a, b' "
                + "CALL { WITH n RETURN n.y AS y } "
                + "RETURN {a: 1, b: 2} AS m, [1, 2] AS l"));
    assertThrows(HopException.class, () -> AgeCypher.getReturnColumns("MATCH (n) RETURN *"));
  }

  @Test
  void testReturnColumnsWithComments() throws Exception {
    // Commas and apostrophes in comments don't split the items, comments aren't part of the names
    assertEquals(
        List.of("id", "name"),
        AgeCypher.getReturnColumns(
            "MATCH (n) RETURN n.id AS id, // the id, isn't it\n"
                + " n.name /* the name, it's */ AS name"));
    assertEquals(
        List.of("n.id", "x"),
        AgeCypher.getReturnColumns("MATCH (n) RETURN n.id /* a, b */, 1 AS x // LIMIT 3"));
    // Comment markers in strings stay
    assertEquals(
        List.of("url"), AgeCypher.getReturnColumns("RETURN 'http://x/*y' + \"a//b\" AS url"));
  }

  @Test
  void testReturnColumnsWithKeywordAliases() throws Exception {
    assertEquals(
        List.of("order", "limit", "skip", "union"),
        AgeCypher.getReturnColumns(
            "MATCH (n) RETURN n.a AS order, n.b AS limit, n.c as skip, n.d AS union"));
    assertEquals(
        List.of("order"),
        AgeCypher.getReturnColumns("MATCH (n) RETURN n.a AS order ORDER BY order LIMIT 3"));
    assertEquals(
        List.of("limit"),
        AgeCypher.getReturnColumns("MATCH (n) RETURN n.a AS /* alias */ limit SKIP 1"));
    // Backticked aliases
    assertEquals(
        List.of("order", "x"),
        AgeCypher.getReturnColumns("MATCH (n) RETURN n.a AS `order`, n.b AS x LIMIT 2"));
    // An alias named return isn't a RETURN clause
    assertEquals(
        List.of("r"), AgeCypher.getReturnColumns("MATCH (n) WITH n AS return RETURN return AS r"));
  }

  @Test
  void testProcedureCallsNeedReturn() throws Exception {
    assertThrows(
        HopException.class, () -> AgeCypher.getReturnColumns("CALL db.labels() YIELD label"));
    assertThrows(
        HopException.class,
        () -> AgeCypher.getReturnColumns("MATCH (n) CALL my.proc(n) YIELD a, b WHERE a > 1"));
    assertThrows(HopException.class, () -> AgeCypher.getReturnColumns("CALL my.proc()"));
    assertEquals(
        List.of("label"), AgeCypher.getReturnColumns("CALL db.labels() YIELD label RETURN label"));
    // Procedure calls followed by other clauses and CALL subqueries return nothing
    assertEquals(List.of(), AgeCypher.getReturnColumns("CALL my.proc() YIELD a CREATE (n {a: a})"));
    assertEquals(
        List.of(),
        AgeCypher.getReturnColumns("MATCH (n) CALL /* sub */ { WITH n CREATE (m {id: n.id}) }"));
  }

  @Test
  void testSql() {
    assertEquals(
        "SELECT * FROM ag_catalog.cypher('hop''s', $hop$ MATCH (n) RETURN n $hop$, ?) AS (c1 agtype)",
        AgeCypher.toSql("hop's", "MATCH (n) RETURN n", List.of("n"), true));
    assertEquals(
        "SELECT * FROM ag_catalog.cypher('g', $hop1$ RETURN '$hop$' $hop1$) AS (v agtype)",
        AgeCypher.toSql("g", "RETURN '$hop$'", List.of(), false));
    assertEquals(
        "SELECT * FROM ag_catalog.cypher('g', $hop$ CREATE (n) $hop$) AS (v agtype)",
        AgeCypher.toSql("g", " CREATE (n) ;\n", List.of(), false));
  }

  @Test
  void testParameters() throws Exception {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("id", 1L);
    row.put("name", "Ann's");
    row.put("day", LocalDate.of(2024, 1, 2));
    row.put("none", null);
    assertEquals(
        "{\"props\":[{\"id\":1,\"name\":\"Ann's\",\"day\":\"2024-01-02\",\"none\":null}]}",
        AgeCypher.toParameterJson(Map.of("props", List.of(row))));
  }

  @Test
  void testValues() throws Exception {
    assertEquals(1L, AgeCypher.toValue("1"));
    assertEquals(1.5d, AgeCypher.toValue("1.5"));
    assertEquals("x::y", AgeCypher.toValue("\"x::y\""));
    GraphNodeValue vertex =
        (GraphNodeValue)
            AgeCypher.toValue(
                "{\"id\": 844424930131969, \"label\": \"Person\", \"properties\": {\"name\": \"A::B\"}}::vertex");
    assertEquals("844424930131969", vertex.id());
    assertEquals(List.of("Person"), vertex.labels());
    assertEquals(Map.of("name", "A::B"), vertex.properties());
    GraphPathValue path =
        (GraphPathValue)
            AgeCypher.toValue(
                "[{\"id\": 1, \"label\": \"A\", \"properties\": {}}::vertex, "
                    + "{\"id\": 2, \"label\": \"R\", \"end_id\": 3, \"start_id\": 1, \"properties\": {\"w\": 1.5}}::edge, "
                    + "{\"id\": 3, \"label\": \"A\", \"properties\": {}}::vertex]::path");
    assertEquals(2, path.nodes().size());
    GraphRelationshipValue edge = path.relationships().get(0);
    assertEquals("R", edge.type());
    assertEquals("1", edge.startNodeId());
    assertEquals("3", edge.endNodeId());
    assertEquals(Map.of("w", 1.5d), edge.properties());
    // A map like a vertex without the suffix stays a map, a list of vertices stays a list
    assertEquals(
        Map.of("id", 1L, "label", "A", "properties", Map.of()),
        AgeCypher.toValue("{\"id\": 1, \"label\": \"A\", \"properties\": {}}"));
    List<?> list =
        (List<?>)
            AgeCypher.toValue(
                "[{\"id\": 1, \"label\": \"A\", \"properties\": {}}::vertex, {\"k\": 2::numeric}]");
    assertEquals(GraphNodeValue.class, list.get(0).getClass());
    assertEquals(Map.of("k", 2L), list.get(1));
  }

  /** AGE prints the float values NaN, Infinity and -Infinity as bare tokens. */
  @Test
  void testNonNumericFloats() throws Exception {
    assertEquals(Double.NaN, AgeCypher.toValue("NaN"));
    assertEquals(Double.POSITIVE_INFINITY, AgeCypher.toValue("Infinity"));
    assertEquals(Double.NEGATIVE_INFINITY, AgeCypher.toValue("-Infinity"));
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("n", Double.NaN);
    expected.put("i", Double.POSITIVE_INFINITY);
    expected.put("m", Double.NEGATIVE_INFINITY);
    expected.put("l", List.of(Double.NaN, 1.5d));
    assertEquals(
        expected,
        AgeCypher.toValue("{\"n\": NaN, \"i\": Infinity, \"m\": -Infinity, \"l\": [NaN, 1.5]}"));
  }

  @Test
  void testIndexedProperties() {
    assertEquals(
        List.of("k"),
        AgeCypher.getIndexedProperties(
            "CREATE UNIQUE INDEX vtest_k ON vtest.\"VTest\" USING btree"
                + " (agtype_access_operator(VARIADIC ARRAY[properties, '\"k\"'::agtype]))"));
    assertEquals(
        List.of("a", "b c"),
        AgeCypher.getIndexedProperties(
            "CREATE INDEX vtest_ab ON vtest.\"VTest\" USING btree"
                + " (agtype_access_operator(VARIADIC ARRAY[properties, '\"a\"'::agtype]),"
                + " agtype_access_operator(VARIADIC ARRAY[properties, '\"b c\"'::agtype]))"));
    assertEquals(
        GraphIndex.ALL_PROPERTIES,
        AgeCypher.getIndexedProperties(
            "CREATE INDEX vtest_gin ON vtest.\"VTest\" USING gin (properties)"));
    assertEquals(
        List.of(),
        AgeCypher.getIndexedProperties(
            "CREATE UNIQUE INDEX \"VTest_pkey\" ON vtest.\"VTest\" USING btree (id)"));
  }
}
