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
package org.apache.hop.ai.transforms.embedtext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EmbedTextTest {

  private TransformMockHelper<EmbedTextMeta, EmbedTextData> helper;
  private List<Object[]> passed;
  private List<Object[]> diverted;
  private boolean failTheProvider;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("EmbedText", EmbedTextMeta.class, EmbedTextData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    passed = new ArrayList<>();
    diverted = new ArrayList<>();
    failTheProvider = false;
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void keepsInputOrderWhenSomeRowsHaveNoTextToEmbed() throws Exception {
    // A row with no text needs no provider call, but emitting it straight away would let it
    // overtake the rows already waiting in the batch.
    run(Arrays.asList(new Object[] {"first"}, new Object[] {""}, new Object[] {"third"}));

    assertEquals(3, passed.size());
    assertEquals("first", passed.get(0)[0]);
    assertEquals("", passed.get(1)[0]);
    assertEquals("third", passed.get(2)[0]);
  }

  @Test
  void embedsOnlyTheRowsThatHaveText() throws Exception {
    run(Arrays.asList(new Object[] {"first"}, new Object[] {""}, new Object[] {"third"}));

    // The embedding column is index 1; the empty row must not get one.
    assertEquals("[1.0,2.0]", passed.get(0)[1]);
    assertNull(passed.get(1)[1], "a row with no text gets no embedding");
    assertEquals("[1.0,2.0]", passed.get(2)[1]);
  }

  @Test
  void passesEveryRowOnEvenWhenNoneHaveText() throws Exception {
    run(Arrays.asList(new Object[] {""}, new Object[] {""}));

    assertEquals(2, passed.size(), "rows with nothing to embed are not dropped");
  }

  @Test
  void divertsOnlyTheRowsThatWereActuallySent() throws Exception {
    // A row with no text is not in the request, so a provider failure is not its failure.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    failTheProvider = true;

    run(Arrays.asList(new Object[] {"first"}, new Object[] {""}, new Object[] {"third"}));

    assertEquals(1, passed.size(), "the row with no text still reaches the output");
    assertEquals("", passed.get(0)[0]);
    assertEquals(2, diverted.size(), "only the two rows that were sent are diverted");
  }

  @Test
  void failsWhenTheOutputFieldNameResolvesToNothing() {
    // getFields adds no embedding column for an empty name, so writing one would run off the row.
    EmbedTextMeta meta = new EmbedTextMeta();
    meta.setDefault();
    meta.setAiProvider("ollama");
    meta.setOutputField("${EMPTY_OUTPUT_FIELD}");

    EmbedText transform =
        new EmbedText(
            helper.transformMeta,
            meta,
            new EmbedTextData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    transform.setVariable("EMPTY_OUTPUT_FIELD", "");

    HopException e =
        assertThrows(HopException.class, () -> transform.resolveOutputFieldNamesForTesting());
    assertTrue(e.getMessage().contains("output field"), e.getMessage());
  }

  private void run(List<Object[]> rows) throws Exception {
    EmbedTextMeta meta = new EmbedTextMeta();
    meta.setDefault();
    meta.setAiProvider("ollama");
    meta.setInputField("chunk_text");
    meta.setIncludeModelMetadata(false);

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("chunk_text"));

    EmbedTextData data = new EmbedTextData();
    data.inputRowMeta = inputRowMeta;
    data.outputRowMeta = inputRowMeta.clone();
    data.outputRowMeta.addValueMeta(new ValueMetaString("embedding"));
    data.inputFieldIndex = 0;
    data.batchSize = 16;
    data.modelName = "test-model";
    data.outputFieldName = "embedding";

    EmbeddingModel model = mock(EmbeddingModel.class);
    when(model.embedAll(any()))
        .thenAnswer(
            invocation -> {
              if (failTheProvider) {
                throw new RuntimeException("provider is down");
              }
              List<TextSegment> segments = invocation.getArgument(0);
              List<Embedding> embeddings = new ArrayList<>();
              for (int i = 0; i < segments.size(); i++) {
                embeddings.add(Embedding.from(new float[] {1.0f, 2.0f}));
              }
              return Response.from(embeddings);
            });
    data.model = model;

    EmbedText transform =
        spy(
            new EmbedText(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    transform.init();
    transform.setInputRowMeta(inputRowMeta);
    // The model and the field indexes are already set up, so skip the first-row branch.
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
