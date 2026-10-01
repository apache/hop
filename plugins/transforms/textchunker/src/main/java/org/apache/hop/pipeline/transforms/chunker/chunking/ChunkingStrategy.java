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
package org.apache.hop.pipeline.transforms.chunker.chunking;

import java.util.List;
import org.apache.hop.pipeline.transforms.chunker.Chunk;

/**
 * Interface for text chunking strategies. Implementations define how text is divided into chunks.
 */
public interface ChunkingStrategy {

  /**
   * Chunks the given text according to the strategy's rules.
   *
   * @param text The text to chunk
   * @param maxSize The maximum size for each chunk (interpretation depends on strategy)
   * @param overlap The number of characters/units to overlap between chunks
   * @return A list of chunks
   */
  List<Chunk> chunk(String text, int maxSize, int overlap);

  /**
   * Gets the type of this chunking strategy.
   *
   * @return The strategy type
   */
  ChunkingStrategyType getType();
}
