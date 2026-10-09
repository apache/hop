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

package org.apache.hop.neo4j.actions.propertygraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphProperty;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.model.GraphRelationship;
import org.junit.jupiter.api.Test;

class PropertyGraphGeneratorTest {

  private static GraphProperty property(String name, GraphPropertyType type, boolean primary) {
    return new GraphProperty(name, null, type, primary, false, false, false);
  }

  /** Person -KNOWS-> Person, Person -WORKS_AT-> Company. */
  static GraphModel model() {
    GraphNode person =
        new GraphNode(
            "Person",
            null,
            new ArrayList<>(List.of("Person")),
            new ArrayList<>(
                List.of(
                    property("id", GraphPropertyType.Integer, true),
                    property("name", GraphPropertyType.String, false))));
    GraphNode company =
        new GraphNode(
            "Company",
            null,
            new ArrayList<>(),
            new ArrayList<>(List.of(property("code", GraphPropertyType.String, true))));
    GraphRelationship knows =
        new GraphRelationship(
            "KNOWS",
            null,
            "KNOWS",
            new ArrayList<>(List.of(property("since", GraphPropertyType.Integer, false))),
            "Person",
            "Person");
    GraphRelationship worksAt =
        new GraphRelationship("WORKS_AT", null, "WORKS_AT", new ArrayList<>(), "Person", "Company");
    return new GraphModel(
        "social",
        null,
        new ArrayList<>(List.of(person, company)),
        new ArrayList<>(List.of(knows, worksAt)));
  }

  @Test
  void testDefaults() throws Exception {
    PropertyGraphGenerator generator = new PropertyGraphGenerator(model(), null, null);
    assertEquals(
        "CREATE PROPERTY GRAPH social\n"
            + "  VERTEX TABLES (\n"
            + "    Person KEY (id) LABEL Person PROPERTIES (id, name),\n"
            + "    Company KEY (code) LABEL Company PROPERTIES (code)\n"
            + "  )\n"
            + "  EDGE TABLES (\n"
            + "    KNOWS KEY (source_id, target_id) SOURCE KEY (source_id) REFERENCES Person (id)"
            + " DESTINATION KEY (target_id) REFERENCES Person (id) LABEL KNOWS PROPERTIES (since),\n"
            + "    WORKS_AT KEY (source_id, target_code) SOURCE KEY (source_id) REFERENCES Person (id)"
            + " DESTINATION KEY (target_code) REFERENCES Company (code) LABEL WORKS_AT NO PROPERTIES\n"
            + "  )",
        generator
            .getCreatePropertyGraphStatement("social", null, false, name -> name)
            .replace("\r", ""));
  }

  @Test
  void testMappingsSchemaAndReplace() throws Exception {
    PropertyGraphGenerator generator =
        new PropertyGraphGenerator(
            model(),
            List.of(new NodeTableMapping("Person", "people", null)),
            List.of(new EdgeTableMapping("KNOWS", "friends", "id", "a", "b")));
    String sql =
        generator
            .getCreatePropertyGraphStatement("g", "hr", true, name -> "\"" + name + "\"")
            .replace("\r", "");
    assertEquals(
        "CREATE OR REPLACE PROPERTY GRAPH \"hr\".\"g\"\n"
            + "  VERTEX TABLES (\n"
            + "    \"hr\".\"people\" AS \"people\" KEY (\"id\") LABEL \"Person\" PROPERTIES (\"id\", \"name\"),\n"
            + "    \"hr\".\"Company\" AS \"Company\" KEY (\"code\") LABEL \"Company\" PROPERTIES (\"code\")\n"
            + "  )\n"
            + "  EDGE TABLES (\n"
            + "    \"hr\".\"friends\" AS \"friends\" KEY (\"id\") SOURCE KEY (\"a\") REFERENCES \"people\" (\"id\")"
            + " DESTINATION KEY (\"b\") REFERENCES \"people\" (\"id\") LABEL \"KNOWS\" PROPERTIES (\"since\"),\n"
            + "    \"hr\".\"WORKS_AT\" AS \"WORKS_AT\" KEY (\"source_id\", \"target_code\") SOURCE KEY (\"source_id\")"
            + " REFERENCES \"people\" (\"id\") DESTINATION KEY (\"target_code\") REFERENCES \"Company\" (\"code\")"
            + " LABEL \"WORKS_AT\" NO PROPERTIES\n"
            + "  )",
        sql);
  }

  @Test
  void testTableColumns() throws Exception {
    PropertyGraphGenerator generator = new PropertyGraphGenerator(model(), null, null);
    PropertyGraphGenerator.EdgeTable knows = generator.getEdgeTables().get(0);
    IRowMeta rowMeta = PropertyGraphGenerator.getRowMeta(knows.columns());
    assertEquals(List.of("source_id", "target_id", "since"), List.of(rowMeta.getFieldNames()));
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(0).getType());

    PropertyGraphGenerator.EdgeTable worksAt = generator.getEdgeTables().get(1);
    IRowMeta worksAtRowMeta = PropertyGraphGenerator.getRowMeta(worksAt.columns());
    // The key column takes the type of the key it refers to
    assertEquals(IValueMeta.TYPE_STRING, worksAtRowMeta.getValueMeta(1).getType());
    assertEquals(255, worksAtRowMeta.getValueMeta(1).getLength());
  }

  @Test
  void testDefaultMappings() throws Exception {
    List<EdgeTableMapping> edges = PropertyGraphGenerator.getDefaultEdgeMappings(model());
    assertEquals("source_id", edges.get(1).getSourceKeyColumns());
    assertEquals("target_code", edges.get(1).getTargetKeyColumns());
    assertEquals(
        "id", PropertyGraphGenerator.getDefaultNodeMappings(model()).get(0).getKeyColumns());
  }

  @Test
  void testErrors() {
    GraphModel noKey = model();
    noKey.getNodes().get(1).getProperties().get(0).setPrimary(false);
    assertThrows(HopException.class, () -> new PropertyGraphGenerator(noKey, null, null));
    assertThrows(
        HopException.class,
        () ->
            new PropertyGraphGenerator(
                model(), List.of(new NodeTableMapping("Person", null, "missing")), null));
    assertThrows(
        HopException.class,
        () ->
            new PropertyGraphGenerator(
                model(), null, List.of(new EdgeTableMapping("KNOWS", null, null, "a, b", null))));
  }

  /** Names which the database quoting can't handle safely are refused before generating SQL. */
  @Test
  void testValidateIdentifiers() throws Exception {
    PropertyGraphGenerator valid = new PropertyGraphGenerator(model(), null, null);
    valid.validateIdentifiers("social", "hr", "\"", "\"");
    valid.validateIdentifiers("my graph", null, "[", "]");

    // The graph name, from a variable for example
    HopException e =
        assertThrows(
            HopException.class,
            () -> valid.validateIdentifiers("g\" AS SELECT 1; DROP TABLE x; --", null, "\"", "\""));
    assertTrue(e.getMessage().contains("the property graph"), e.getMessage());

    // The quote characters of the database
    assertThrows(HopException.class, () -> valid.validateIdentifiers("a]b", null, "[", "]"));
    assertThrows(HopException.class, () -> valid.validateIdentifiers("social", "h`r", "`", "`"));
    // A statement terminator or a line break
    assertThrows(HopException.class, () -> valid.validateIdentifiers("a;b", null, "\"", "\""));
    assertThrows(HopException.class, () -> valid.validateIdentifiers("a\nb", null, "\"", "\""));

    // A table name from a mapping
    PropertyGraphGenerator table =
        new PropertyGraphGenerator(
            model(), List.of(new NodeTableMapping("Person", "people'", null)), null);
    e = assertThrows(HopException.class, () -> table.validateIdentifiers("g", null, "\"", "\""));
    assertTrue(e.getMessage().contains("node 'Person'"), e.getMessage());

    // Source key columns of an edge table
    PropertyGraphGenerator sourceKey =
        new PropertyGraphGenerator(
            model(), null, List.of(new EdgeTableMapping("KNOWS", null, null, "a\"b", null)));
    e =
        assertThrows(
            HopException.class, () -> sourceKey.validateIdentifiers("g", null, "\"", "\""));
    assertTrue(e.getMessage().contains("relationship 'KNOWS'"), e.getMessage());

    // A label and a property of the graph model
    GraphModel badLabel = model();
    badLabel.getNodes().get(0).getLabels().set(0, "Per\"son");
    assertThrows(
        HopException.class,
        () ->
            new PropertyGraphGenerator(badLabel, null, null)
                .validateIdentifiers("g", null, "\"", "\""));
    GraphModel badProperty = model();
    badProperty.getRelationships().get(0).getProperties().get(0).setName("since;");
    assertThrows(
        HopException.class,
        () ->
            new PropertyGraphGenerator(badProperty, null, null)
                .validateIdentifiers("g", null, "\"", "\""));
  }
}
