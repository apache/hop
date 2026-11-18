/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.neo4j.core.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.neo4j.core.data.GraphData;
import org.apache.hop.neo4j.core.data.GraphNodeData;
import org.apache.hop.neo4j.core.data.GraphPropertyData;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.core.data.GraphRelationshipData;
import org.junit.jupiter.api.Test;

/** A graph comes back from the text an engine carries it as. */
class ValueMetaGraphTest {

  private static GraphData graph() {
    List<GraphPropertyData> properties = new ArrayList<>();
    properties.add(new GraphPropertyData("name", "Neo", GraphPropertyDataType.String, true));
    GraphNodeData neo = new GraphNodeData("n1", List.of("Person"), properties);
    GraphNodeData trinity = new GraphNodeData("n2", List.of("Person"), new ArrayList<>());
    GraphRelationshipData knows =
        new GraphRelationshipData("r1", "KNOWS", new ArrayList<>(), "n1", "n2");
    return new GraphData(List.of(neo, trinity), List.of(knows));
  }

  @Test
  void aGraphComesBackFromItsText() throws Exception {
    ValueMetaGraph valueMeta = new ValueMetaGraph("graph");
    GraphData graph = graph();
    String text = valueMeta.getString(graph);

    Object back = valueMeta.convertData(new ValueMetaString("text"), text);

    assertEquals(text, ((GraphData) back).toJsonString());
  }

  /** Text file input, CSV input and the like read a Graph field this way. */
  @Test
  void aGraphIsReadFromText() throws Exception {
    ValueMetaGraph valueMeta = new ValueMetaGraph("graph");
    String text = valueMeta.getString(graph());

    Object read =
        valueMeta.convertDataFromString(
            text, new ValueMetaString("text"), null, null, IValueMeta.TRIM_TYPE_NONE);

    assertEquals(text, ((GraphData) read).toJsonString());
  }

  @Test
  void aGraphIsCopied() throws Exception {
    ValueMetaGraph valueMeta = new ValueMetaGraph("graph");
    GraphData graph = graph();

    Object copy = valueMeta.convertData(new ValueMetaGraph("other"), graph);

    assertEquals(graph.toJsonString(), ((GraphData) copy).toJsonString());
    assertNotSame(graph, copy);
  }

  @Test
  void emptyTextIsNoGraph() throws Exception {
    ValueMetaGraph valueMeta = new ValueMetaGraph("graph");

    assertNull(valueMeta.convertData(new ValueMetaString("text"), ""));
    assertNull(valueMeta.convertData(new ValueMetaString("text"), null));
  }

  @Test
  void textThatIsNoGraphOrAnotherTypeIsAnError() {
    ValueMetaGraph valueMeta = new ValueMetaGraph("graph");

    assertThrows(
        HopValueException.class,
        () -> valueMeta.convertData(new ValueMetaString("text"), "not json"));
    assertThrows(
        HopValueException.class, () -> valueMeta.convertData(new ValueMetaInteger("n"), 1L));
  }
}
