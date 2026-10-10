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

package org.apache.hop.neo4j.transforms.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Labels and relationship types from field values are always used as names, never as Cypher. */
class Neo4JOutputLabelTest {

  private TransformMockHelper<Neo4JOutputMeta, Neo4JOutputData> helper;
  private Neo4JOutput output;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("Neo4j Output", Neo4JOutputMeta.class, Neo4JOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    output =
        new Neo4JOutput(
            helper.transformMeta,
            new Neo4JOutputMeta(),
            new Neo4JOutputData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void labelsAreQuoted() {
    assertEquals("`Person`", output.escapeLabel("Person"));
    assertEquals("`Big Company`", output.escapeLabel("Big Company"));
    assertEquals("`my-label`", output.escapeLabel("my-label"));
    assertEquals("`a.b`", output.escapeLabel("a.b"));
  }

  @Test
  void backticksAreDoubled() {
    assertEquals("`we``ird`", output.escapeLabel("we`ird"));
    assertEquals(
        "`X`` {a:1})-[:R]->() DETACH DELETE f //`",
        output.escapeLabel("X` {a:1})-[:R]->() DETACH DELETE f //"));
    assertEquals("```a`` DELETE ``b```", output.escapeLabel("`a` DELETE `b`"));
  }

  @Test
  void quotedLabelsStayAsTheyAre() {
    assertEquals("`Person`", output.escapeLabel("`Person`"));
  }
}
