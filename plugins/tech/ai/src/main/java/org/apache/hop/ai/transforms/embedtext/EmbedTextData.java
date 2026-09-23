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

import dev.langchain4j.model.embedding.EmbeddingModel;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class EmbedTextData extends BaseTransformData implements ITransformData {

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;

  public EmbeddingModel model;

  /** Model name actually in use, for the optional output field and for error messages. */
  public String modelName;

  /** Batch size resolved once in init, so a variable is not re-resolved per row. */
  public int batchSize;

  /** True when the embedding is emitted as a Vector field rather than a JSON string. */
  public boolean emitVector;

  public int inputFieldIndex = -1;

  /**
   * Output field names resolved once in the first row, so the layout {@code getFields} produced and
   * the slots written here are decided by the same values. Judging them separately lets a variable
   * that resolves to empty put a value in the wrong column.
   */
  public String outputFieldName;

  public String modelFieldName;

  public String dimensionsFieldName;

  /**
   * Rows waiting to be released, in input order. They are only passed downstream once the provider
   * has answered, so no downstream transform sees a row before its embedding exists.
   *
   * <p>A row whose text is empty needs no embedding, but it still waits here rather than being
   * emitted straight away: letting it overtake the rows already buffered would reorder the stream.
   * Its entry in {@link #pendingTexts} is null.
   */
  public final List<Object[]> pendingRows = new ArrayList<>();

  public final List<String> pendingTexts = new ArrayList<>();

  public EmbedTextData() {
    super();
  }
}
