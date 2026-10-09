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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExtractGraphTest {

  private TransformMockHelper<ExtractGraphMeta, ExtractGraphData> helper;
  private List<Object[]> passed;
  private List<Object[]> diverted;
  private List<ChatRequest> requests;
  private String answer;
  private boolean passRowsWithoutResults;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("ExtractGraph", ExtractGraphMeta.class, ExtractGraphData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    passed = new ArrayList<>();
    diverted = new ArrayList<>();
    requests = new ArrayList<>();
    answer =
        "{\"entities\": [{\"name\": \"Ada\", \"type\": \"Person\", \"description\": \"d1\"},"
            + " {\"name\": \"Engine\", \"type\": \"Machine\", \"description\": \"d2\"}],"
            + " \"relationships\": [{\"source\": \"Ada\", \"target\": \"Engine\","
            + " \"type\": \"PROGRAMMED\", \"description\": \"d3\"}]}";
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void writesARowPerEntityAndRelationshipKeepingTheInputFields() throws Exception {
    run(List.<Object[]>of(new Object[] {"doc-1", "Ada programmed the engine."}));

    assertEquals(3, passed.size());
    // input fields, then kind, name, type, description, source, target
    assertEquals("doc-1", passed.get(0)[0]);
    assertEquals(ExtractGraphMeta.KIND_ENTITY, passed.get(0)[2]);
    assertEquals("Ada", passed.get(0)[3]);
    assertEquals("Person", passed.get(0)[4]);
    Object[] relationship = passed.get(2);
    assertEquals(ExtractGraphMeta.KIND_RELATIONSHIP, relationship[2]);
    assertNull(relationship[3]);
    assertEquals("PROGRAMMED", relationship[4]);
    assertEquals("Ada", relationship[6]);
    assertEquals("Engine", relationship[7]);
  }

  @Test
  void aRowWithoutTextProducesNothingAndCallsNoModel() throws Exception {
    run(Arrays.asList(new Object[] {"doc-1", ""}, new Object[] {"doc-2", "text"}));

    assertEquals(3, passed.size());
    assertEquals("doc-2", passed.get(0)[0]);
    assertEquals(1, requests.size());
  }

  @Test
  void aRowWithoutTextIsPassedThroughWhenAsked() throws Exception {
    passRowsWithoutResults = true;
    run(Arrays.asList(new Object[] {"doc-1", ""}, new Object[] {"doc-2", "text"}));

    assertEquals(4, passed.size());
    Object[] empty = passed.get(0);
    assertEquals("doc-1", empty[0]);
    for (int i = 2; i < 8; i++) {
      assertNull(empty[i]);
    }
    assertEquals(1, requests.size());
  }

  @Test
  void anAnswerWithoutElementsProducesNothingUnlessAsked() throws Exception {
    answer = "{\"entities\": [], \"relationships\": []}";
    run(List.<Object[]>of(new Object[] {"doc-1", "text"}));
    assertEquals(0, passed.size());

    passRowsWithoutResults = true;
    run(List.<Object[]>of(new Object[] {"doc-1", "text"}));
    assertEquals(1, passed.size());
    assertEquals("doc-1", passed.get(0)[0]);
    assertNull(passed.get(0)[2]);
  }

  @Test
  void anOffListTypeDropsOnlyThatElement() throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    // Rock is not an allowed type; machine is, ignoring case
    answer =
        "{\"entities\": [{\"name\": \"Ada\", \"type\": \"Person\", \"description\": \"d1\"},"
            + " {\"name\": \"Engine\", \"type\": \"machine\", \"description\": \"d2\"},"
            + " {\"name\": \"Granite\", \"type\": \"Rock\", \"description\": \"d3\"}],"
            + " \"relationships\": [{\"source\": \"Ada\", \"target\": \"Granite\","
            + " \"type\": \"LIKES\", \"description\": \"d4\"}]}";

    run(List.<Object[]>of(new Object[] {"doc-1", "text"}));

    assertEquals(0, diverted.size());
    assertEquals(2, passed.size());
    assertEquals("Machine", passed.get(1)[4]);
  }

  @Test
  void anUnreadableAnswerGoesToTheErrorHop() throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    answer = "Sorry, no graph today.";

    run(List.<Object[]>of(new Object[] {"doc-1", "text"}));

    assertEquals(0, passed.size());
    assertEquals(1, diverted.size());
  }

  @Test
  void theTypesReachTheModel() throws Exception {
    run(List.<Object[]>of(new Object[] {"doc-1", "text"}));

    String system = requests.get(0).messages().get(0).toString();
    assertTrue(system.contains("Person, Machine"), system);
  }

  @Test
  void outputFieldsFollowTheInput() throws Exception {
    ExtractGraphMeta meta = new ExtractGraphMeta();
    meta.setDefault();
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("chunk_text"));
    meta.getFields(row, "x", null, null, new Variables(), null);
    assertEquals(
        List.of("chunk_text", "graph_element", "name", "type", "description", "source", "target"),
        Arrays.asList(row.getFieldNames()));
  }

  private void run(List<Object[]> rows) throws Exception {
    passed.clear();
    requests.clear();
    ExtractGraphMeta meta = new ExtractGraphMeta();
    meta.setDefault();
    meta.setAiProvider("ollama-local");
    meta.setInputField("text");
    meta.setEntityTypes("Person, Machine");
    meta.setPassRowsWithoutResults(passRowsWithoutResults);

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("document_id"));
    inputRowMeta.addValueMeta(new ValueMetaString("text"));

    ExtractGraphData data = new ExtractGraphData();
    data.inputRowMeta = inputRowMeta;
    data.outputRowMeta = inputRowMeta.clone();
    meta.getFields(data.outputRowMeta, "x", null, null, new Variables(), null);
    data.inputFieldIndex = 1;

    ChatModel model = mock(ChatModel.class);
    when(model.chat(any(ChatRequest.class)))
        .thenAnswer(
            invocation -> {
              requests.add(invocation.getArgument(0));
              return ChatResponse.builder().aiMessage(AiMessage.from(answer)).build();
            });
    data.model = model;

    ExtractGraph transform =
        spy(
            new ExtractGraph(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    transform.init();
    data.systemPrompt = GraphExtraction.systemPrompt(data.entityTypes, data.relationshipTypes, "");
    transform.setInputRowMeta(inputRowMeta);
    // The model and the indexes are already set up, so skip the first-row branch.
    transform.first = false;

    Iterator<Object[]> iterator = rows.iterator();
    doAnswer(invocation -> iterator.hasNext() ? iterator.next() : null).when(transform).getRow();
    doAnswer(
            invocation -> {
              passed.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any(Object[].class));
    doAnswer(
            invocation -> {
              diverted.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putError(any(), any(), anyLong(), any(), any(), any());

    while (transform.processRow()) {
      // drain
    }
  }
}
