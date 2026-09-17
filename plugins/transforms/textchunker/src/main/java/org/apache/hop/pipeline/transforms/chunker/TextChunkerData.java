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

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/** Data class for the TextChunker transform. Holds runtime data that persists between rows. */
public class TextChunkerData extends BaseTransformData implements ITransformData {

  /** The input row metadata. */
  public IRowMeta inputRowMeta;

  /** The output row metadata. */
  public IRowMeta outputRowMeta;

  /** The index of the input field containing text to chunk. */
  /** Chunk size and overlap resolved once in init, so variables are not re-resolved per row. */
  public int chunkSize;

  public int chunkOverlap;

  public int inputFieldIndex = -1;

  /** The index of the optional input field carrying the business document identifier. */
  public int sourceDocumentIdFieldIndex = -1;

  /** The index of the optional input field carrying the content type hint. */
  public int contentTypeFieldIndex = -1;

  /** The index of the output chunk field, resolved once against {@link #outputRowMeta}. */
  public int outputChunkFieldIndex = -1;

  /** The index of the chunk index field (if metadata is enabled). */
  public int chunkIndexFieldIndex = -1;

  /** The index of the chunk start position field (if metadata is enabled). */
  public int chunkStartPosFieldIndex = -1;

  /** The index of the original document ID field (if metadata is enabled). */
  public int documentIdFieldIndex = -1;

  /** The index of the total chunk count field (if metadata is enabled). */
  public int chunkCountFieldIndex = -1;

  public TextChunkerData() {
    super();
  }
}
