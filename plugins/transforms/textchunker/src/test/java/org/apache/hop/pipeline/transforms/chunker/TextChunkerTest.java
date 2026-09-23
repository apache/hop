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
package org.apache.hop.pipeline.transforms.chunker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.chunker.chunking.ChunkingStrategyType;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TextChunkerTest {

  private TransformMockHelper<TextChunkerMeta, TextChunkerData> helper;
  private List<Object[]> output;
  private IRowMeta outputRowMeta;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("TextChunker", TextChunkerMeta.class, TextChunkerData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    output = new ArrayList<>();
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void emitsOneRowPerChunkWithNumericMetadata() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");
    meta.setChunkingStrategy(ChunkingStrategyType.CHARACTER);
    meta.setChunkSize("10");
    meta.setChunkOverlap("0");

    // 3 words of 9 characters each, so the character strategy produces several chunks.
    run(meta, rowMeta(), List.<Object[]>of(new Object[] {"aaaaaaaa bbbbbbbb cccccccc"}));

    assertTrue(output.size() > 1, "expected the text to be split into multiple chunks");

    int indexPos = outputRowMeta.indexOfValue("chunk_index");
    int startPos = outputRowMeta.indexOfValue("chunk_start_position");
    int totalPos = outputRowMeta.indexOfValue("total_chunks");
    int docPos = outputRowMeta.indexOfValue("chunk_doc_id");

    for (int i = 0; i < output.size(); i++) {
      Object[] row = output.get(i);
      assertEquals(Long.valueOf(i), row[indexPos], "chunk_index must be a sequential Long");
      assertTrue(row[startPos] instanceof Long, "chunk_start_position must be a Long");
      assertEquals(
          Long.valueOf(output.size()), row[totalPos], "total_chunks must be a Long and correct");
      assertNotNull(row[docPos]);
    }
  }

  @Test
  void usesTheSourceDocumentIdFieldWhenConfigured() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");
    meta.setSourceDocumentIdField("doc");
    meta.setChunkSize("1000");

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("text"));
    rowMeta.addValueMeta(new ValueMetaString("doc"));

    run(meta, rowMeta, List.<Object[]>of(new Object[] {"short text", "DOC-42"}));

    int docPos = outputRowMeta.indexOfValue("chunk_doc_id");
    assertEquals(1, output.size());
    assertEquals("DOC-42", output.get(0)[docPos]);
  }

  /** Document IDs are prefixed with the copy number so parallel copies cannot collide. */
  @Test
  void generatedDocumentIdsAreUniquePerCopy() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");
    meta.setChunkSize("1000");

    run(meta, rowMeta(), List.<Object[]>of(new Object[] {"one"}, new Object[] {"two"}));

    int docPos = outputRowMeta.indexOfValue("chunk_doc_id");
    assertEquals(2, output.size());
    assertEquals("0_0", output.get(0)[docPos]);
    assertEquals("0_1", output.get(1)[docPos]);
  }

  @Test
  void emitsASingleRowForEmptyText() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");

    run(meta, rowMeta(), List.<Object[]>of(new Object[] {""}));

    int chunkPos = outputRowMeta.indexOfValue("chunk_text");
    int totalPos = outputRowMeta.indexOfValue("total_chunks");
    assertEquals(1, output.size());
    assertEquals("", output.get(0)[chunkPos]);
    assertEquals(0L, output.get(0)[totalPos]);
  }

  private static IRowMeta rowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("text"));
    return rowMeta;
  }

  private void run(TextChunkerMeta meta, IRowMeta inputRowMeta, List<Object[]> rows)
      throws Exception {
    TextChunkerData data = new TextChunkerData();
    TextChunker transform =
        spy(
            new TextChunker(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    transform.init();
    transform.setInputRowMeta(inputRowMeta);

    Iterator<Object[]> iterator = rows.iterator();
    doAnswer(invocation -> iterator.hasNext() ? iterator.next() : null).when(transform).getRow();
    doAnswer(
            invocation -> {
              outputRowMeta = invocation.getArgument(0);
              output.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any(Object[].class));

    while (transform.processRow()) {
      // drain
    }
  }

  /**
   * Cross-module contract: PgVectorUpsert reads chunk_index back out with getString() and parses it
   * as an int. Now that the chunker emits it as an Integer rather than a String, the default
   * conversion must still yield plain digits with no mask or grouping separator.
   */
  @Test
  void integerChunkMetadataStringifiesToPlainDigits() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("chunk_index"));

    for (long value : new long[] {0L, 5L, 42L, 1234L, 1234567L}) {
      String rendered = rowMeta.getString(new Object[] {value}, 0);
      assertEquals(
          Long.toString(value), rendered, "chunk_index must render without mask or grouping");
      assertEquals(value, Integer.parseInt(rendered.trim()));
    }
  }

  @Test
  void resolvesChunkSizeAndOverlapFromVariables() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");
    meta.setChunkingStrategy(ChunkingStrategyType.CHARACTER);
    meta.setChunkSize("${CHUNK_SIZE}");
    meta.setChunkOverlap("${CHUNK_OVERLAP}");

    TextChunkerData data = new TextChunkerData();
    TextChunker transform =
        new TextChunker(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.setVariable("CHUNK_SIZE", "10");
    transform.setVariable("CHUNK_OVERLAP", "3");

    assertTrue(transform.init(), "init must succeed once the variables resolve");
    assertEquals(10, data.chunkSize);
    assertEquals(3, data.chunkOverlap);
  }

  @Test
  void initFailsWhenTheResolvedChunkSizeIsNotPositive() {
    // Metadata injection can set this to 0 even though check() reports an error.
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");
    meta.setChunkSize("0");

    TextChunkerData data = new TextChunkerData();
    TextChunker transform =
        new TextChunker(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);

    assertFalse(transform.init(), "a non-positive chunk size must fail init, not drop rows");
  }
}
