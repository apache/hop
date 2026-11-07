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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import dev.langchain4j.exception.AuthenticationException;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.net.ConnectException;
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
  private List<String> errorDescriptions;
  private RuntimeException failure;
  private int failuresLeft;
  private int calls;

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
    errorDescriptions = new ArrayList<>();
    failure = null;
    failuresLeft = 0;
    calls = 0;
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
  void treatsWhitespaceOnlyTextAsEmpty() throws Exception {
    // langchain4j refuses a blank text segment, which would stop the transform outright.
    run(Arrays.asList(new Object[] {"first"}, new Object[] {" \t\n "}, new Object[] {"third"}));

    assertEquals(3, passed.size());
    assertEquals("[1.0,2.0]", passed.get(0)[1]);
    assertEquals(" \t\n ", passed.get(1)[0], "the blank text itself is passed on unchanged");
    assertNull(passed.get(1)[1], "a row with blank text gets no embedding");
    assertEquals("[1.0,2.0]", passed.get(2)[1]);
  }

  @Test
  void findsNothingToEmbedInTextThatOnlyHoldsWhitespaceOrControlCharacters() {
    assertTrue(EmbedText.hasNothingToEmbed(null));
    assertTrue(EmbedText.hasNothingToEmbed(""));
    assertTrue(EmbedText.hasNothingToEmbed(" \t\r\n"));
    // langchain4j trims these away and then refuses the segment.
    assertTrue(EmbedText.hasNothingToEmbed("\u0000\u0007"));
    // Unicode spaces that trim keeps.
    assertTrue(EmbedText.hasNothingToEmbed("\u2003\u3000"));
    assertFalse(EmbedText.hasNothingToEmbed(" a "));
  }

  @Test
  void findsNothingToEmbedInNoBreakAndZeroWidthSpaces() {
    // isBlank() keeps all of these, so they used to be sent and came back as a meaningless vector.
    assertTrue(EmbedText.hasNothingToEmbed("\u00A0\u00A0"), "no-break spaces");
    assertTrue(EmbedText.hasNothingToEmbed("\u202F"), "narrow no-break space");
    assertTrue(EmbedText.hasNothingToEmbed("\u2007"), "figure space");
    assertTrue(EmbedText.hasNothingToEmbed("\u200B"), "zero-width space");
    assertTrue(EmbedText.hasNothingToEmbed("\uFEFF \u2060"), "byte order mark and word joiner");
    assertFalse(EmbedText.hasNothingToEmbed("\u200Bword\u00A0"));
    assertFalse(EmbedText.hasNothingToEmbed("\u4E2D"), "a CJK character is text");
  }

  @Test
  void doesNotSendTextOfOnlyUnicodeSpaces() throws Exception {
    run(Arrays.asList(new Object[] {"first"}, new Object[] {"\u00A0\u202F\u200B"}));

    assertEquals(2, passed.size());
    assertEquals("[1.0,2.0]", passed.get(0)[1]);
    assertNull(passed.get(1)[1], "a row of Unicode spaces gets no embedding");
  }

  @Test
  void refusesABatchSizeThatIsNotANumber() {
    // An unset variable stays as ${...}; it used to run with 16 without a word.
    assertFalse(initWithBatchSize("${EMBED_BATCH_NOT_SET}"));
    assertFalse(initWithBatchSize("ten"));
  }

  @Test
  void usesTheDefaultBatchSizeWhenItIsEmptyAndTheValueWhenItIsSet() {
    EmbedTextData data = new EmbedTextData();
    assertTrue(initWithBatchSize("", data));
    assertEquals(EmbedText.DEFAULT_BATCH_SIZE, data.batchSize);

    data = new EmbedTextData();
    assertTrue(initWithBatchSize(" 8 ", data));
    assertEquals(8, data.batchSize);
  }

  @Test
  void namesTheProviderAndEndpointWhenTheProviderCannotBeReached() throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    failWith(new RuntimeException(new ConnectException()), Integer.MAX_VALUE);

    run(Arrays.asList(new Object[] {"first"}, new Object[] {"second"}));

    assertEquals(2, diverted.size());
    String description = errorDescriptions.get(0);
    assertTrue(description.contains("test-model"), description);
    assertTrue(description.contains("'local-ollama'"), description);
    assertTrue(description.contains("http://localhost:11499"), description);
    assertTrue(description.contains("ConnectException"), description);
  }

  @Test
  void stopsWithTheProviderAndEndpointWhenThereIsNoErrorHop() {
    failWith(new RuntimeException(new ConnectException()), Integer.MAX_VALUE);

    HopException e =
        assertThrows(HopException.class, () -> run(List.<Object[]>of(new Object[] {"first"})));
    assertTrue(e.getMessage().contains("'local-ollama' at http://localhost:11499"), e.getMessage());
    assertEquals(1 + EmbedText.MAX_RETRIES, calls, "the first attempt and every retry");
  }

  @Test
  void retriesAFailedCallAndKeepsTheEmbeddings() throws Exception {
    failWith(new RuntimeException("provider is busy"), EmbedText.MAX_RETRIES);

    run(Arrays.asList(new Object[] {"first"}, new Object[] {"second"}));

    assertEquals(1 + EmbedText.MAX_RETRIES, calls);
    assertEquals(2, passed.size());
    assertEquals("[1.0,2.0]", passed.get(1)[1]);
  }

  @Test
  void doesNotRetryARefusalThatWillNotChange() {
    failWith(new AuthenticationException("invalid api key"), Integer.MAX_VALUE);

    assertThrows(HopException.class, () -> run(List.<Object[]>of(new Object[] {"first"})));
    assertEquals(1, calls);
  }

  @Test
  void leavesCredentialsOutOfTheEndpointInMessages() {
    assertEquals(
        "https://host:8443/v1",
        EmbedText.endpointForMessages("https://user:secret@host:8443/v1?api_key=abc#x"));
    assertEquals("http://localhost:11434", EmbedText.endpointForMessages("http://localhost:11434"));
    assertEquals("", EmbedText.endpointForMessages(""));
  }

  @Test
  void divertsOnlyTheRowsThatWereActuallySent() throws Exception {
    // A row with no text is not in the request, so a provider failure is not its failure.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    failWith(new RuntimeException("provider is down"), Integer.MAX_VALUE);

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

  private void failWith(RuntimeException exception, int times) {
    failure = exception;
    failuresLeft = times;
  }

  private boolean initWithBatchSize(String batchSize) {
    return initWithBatchSize(batchSize, new EmbedTextData());
  }

  private boolean initWithBatchSize(String batchSize, EmbedTextData data) {
    EmbedTextMeta meta = new EmbedTextMeta();
    meta.setDefault();
    meta.setAiProvider("ollama");
    meta.setInputField("chunk_text");
    meta.setBatchSize(batchSize);
    EmbedText transform =
        new EmbedText(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    return transform.init();
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
    data.providerName = "local-ollama";
    data.baseUrl = "http://localhost:11499";
    data.retryDelayMillis = 0L;
    data.outputFieldName = "embedding";

    EmbeddingModel model = mock(EmbeddingModel.class);
    when(model.embedAll(any()))
        .thenAnswer(
            invocation -> {
              calls++;
              if (failuresLeft > 0) {
                failuresLeft--;
                throw failure;
              }
              List<TextSegment> segments = invocation.getArgument(0);
              // Like the providers, reject blank input for the whole request.
              for (TextSegment segment : segments) {
                if (segment.text().isBlank()) {
                  throw new RuntimeException("input must not be blank");
                }
              }
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
              errorDescriptions.add(invocation.getArgument(3));
              return null;
            })
        .when(transform)
        .putError(any(), any(), anyLong(), any(), any(), any());

    while (transform.processRow()) {
      // drain
    }
  }
}
