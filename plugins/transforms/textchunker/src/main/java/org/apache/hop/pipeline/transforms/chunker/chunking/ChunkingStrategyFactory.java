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

/** Factory for creating chunking strategy instances based on strategy type. */
public final class ChunkingStrategyFactory {

  private ChunkingStrategyFactory() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates a chunking strategy based on the given type.
   *
   * @param type The strategy type
   * @return The corresponding chunking strategy
   */
  public static ChunkingStrategy createStrategy(ChunkingStrategyType type) {
    if (type == null) {
      return createDefaultStrategy();
    }

    switch (type) {
      case CHARACTER:
        return new CharacterChunkingStrategy();
      case PARAGRAPH:
        return new ParagraphChunkingStrategy();
      case STRUCTURE:
        return new StructureChunkingStrategy();
      default:
        return createDefaultStrategy();
    }
  }

  /**
   * Creates a chunking strategy based on the given type name.
   *
   * @param typeName The strategy type name
   * @return The corresponding chunking strategy
   */
  public static ChunkingStrategy createStrategy(String typeName) {
    return createStrategy(ChunkingStrategyType.fromString(typeName));
  }

  /**
   * Creates the default chunking strategy (Character).
   *
   * @return The default strategy
   */
  public static ChunkingStrategy createDefaultStrategy() {
    return new CharacterChunkingStrategy();
  }
}
