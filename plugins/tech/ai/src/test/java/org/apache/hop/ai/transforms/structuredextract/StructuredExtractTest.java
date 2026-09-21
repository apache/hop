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
package org.apache.hop.ai.transforms.structuredextract;

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
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StructuredExtractTest {

  private TransformMockHelper<StructuredExtractMeta, StructuredExtractData> helper;
  private List<Object[]> passed;
  private List<Object[]> diverted;
  private List<ChatRequest> requests;
  private String answer;
  private String instructions;
  private RuntimeException failure;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "StructuredExtract", StructuredExtractMeta.class, StructuredExtractData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    passed = new ArrayList<>();
    diverted = new ArrayList<>();
    requests = new ArrayList<>();
    answer = "{\"severity\":\"high\",\"total\":12.5}";
    instructions = "";
    failure = null;
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void writesOneTypedValuePerField() throws Exception {
    run(List.<Object[]>of(new Object[] {"the server is down and it cost us 12.50"}));

    assertEquals(1, passed.size());
    assertEquals("high", passed.get(0)[1]);
    assertEquals(12.5d, passed.get(0)[2]);
  }

  @Test
  void passesARowWithNoTextStraightThrough() throws Exception {
    // An empty input is not a failure, and calling a model with nothing to read would be waste.
    run(Arrays.asList(new Object[] {""}, new Object[] {"a real ticket"}));

    assertEquals(2, passed.size());
    assertNull(passed.get(0)[1], "no field is extracted from nothing");
    assertEquals(1, requests.size(), "the model is only asked about the row that has text");
  }

  @Test
  void sendsTheFieldDescriptionsToTheModel() throws Exception {
    // The descriptions are the user's own words and the main lever on extraction quality, so they
    // have to reach the model rather than only shaping the schema.
    run(List.<Object[]>of(new Object[] {"a ticket"}));

    String system = requests.get(0).messages().get(0).toString();
    assertTrue(system.contains("how urgent the ticket is"), system);
    assertTrue(system.contains("amount in euros"), system);
  }

  @Test
  void divertsARowWhenTheModelAnswersWithNonsense() throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    answer = "I am afraid I cannot help with that.";

    run(List.<Object[]>of(new Object[] {"a ticket"}));

    assertEquals(0, passed.size());
    assertEquals(1, diverted.size(), "an unreadable answer is the row's problem, not the run's");
  }

  @Test
  void divertsARowWhenAValueWillNotCoerce() throws Exception {
    // The crucial case: a plausible-looking answer with a bad value must not become a silent null.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    answer = "{\"severity\":\"high\",\"total\":\"about twelve euros\"}";

    run(List.<Object[]>of(new Object[] {"a ticket"}));

    assertEquals(0, passed.size());
    assertEquals(1, diverted.size());
  }

  @Test
  void divertsARowWhenTheProviderFails() throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    failure = new RuntimeException("provider is down");

    run(List.<Object[]>of(new Object[] {"a ticket"}));

    assertEquals(1, diverted.size());
  }

  @Test
  void oneBadRowDoesNotStopTheOthers() throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    List<String> answers =
        new ArrayList<>(
            List.of(
                "{\"severity\":\"low\",\"total\":1}",
                "not json at all",
                "{\"severity\":\"high\",\"total\":2}"));

    runWithAnswers(
        Arrays.asList(new Object[] {"one"}, new Object[] {"two"}, new Object[] {"three"}), answers);

    assertEquals(2, passed.size());
    assertEquals(1, diverted.size());
  }

  @Test
  void extraInstructionsReachTheModel() throws Exception {
    instructions = "The tickets are written in Dutch.";

    run(List.<Object[]>of(new Object[] {"a ticket"}));

    String system = requests.get(0).messages().get(0).toString();
    assertTrue(system.contains("written in Dutch"), system);
  }

  private void run(List<Object[]> rows) throws Exception {
    runWithAnswers(rows, null);
  }

  private void runWithAnswers(List<Object[]> rows, List<String> answers) throws Exception {
    StructuredExtractMeta meta = new StructuredExtractMeta();
    meta.setDefault();
    meta.setAiProvider("openai");
    meta.setInputField("ticket_text");
    List<StructuredExtractField> fields =
        List.of(
            new StructuredExtractField("severity", "String", "how urgent the ticket is", true),
            new StructuredExtractField("total", "Number", "amount in euros", false));
    meta.setInstructions(instructions);
    meta.setFields(new ArrayList<>(fields));

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("ticket_text"));

    StructuredExtractData data = new StructuredExtractData();
    data.inputRowMeta = inputRowMeta;
    data.outputRowMeta = inputRowMeta.clone();
    data.outputRowMeta.addValueMeta(new ValueMetaString("severity"));
    data.outputRowMeta.addValueMeta(new ValueMetaNumber("total"));
    data.inputFieldIndex = 0;
    data.fields = fields;
    data.schemaDescription = SchemaPrompt.describe(fields);
    data.systemPrompt =
        StructuredExtract.buildSystemPrompt(data.schemaDescription, meta.getInstructions());

    Iterator<String> answerIterator = answers == null ? null : answers.iterator();
    ChatModel model = mock(ChatModel.class);
    when(model.chat(any(ChatRequest.class)))
        .thenAnswer(
            invocation -> {
              requests.add(invocation.getArgument(0));
              if (failure != null) {
                throw failure;
              }
              String text =
                  answerIterator != null && answerIterator.hasNext()
                      ? answerIterator.next()
                      : answer;
              return ChatResponse.builder().aiMessage(AiMessage.from(text)).build();
            });
    data.model = model;

    StructuredExtract transform =
        spy(
            new StructuredExtract(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    transform.init();
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
