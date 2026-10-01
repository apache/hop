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
package org.apache.hop.pipeline.transforms.chunker.document;

/** Maps RAG corpus {@code source_type} values to document parsers. */
public final class ContentTypeResolver {

  private ContentTypeResolver() {}

  /**
   * Map hop-rag-corpus {@code source_type} to a content type. Returns {@link ContentType#AUTO} when
   * unknown.
   */
  public static ContentType fromSourceType(String sourceType) {
    if (sourceType == null || sourceType.isBlank()) {
      return ContentType.AUTO;
    }
    return switch (sourceType.trim().toLowerCase()) {
      case "doc" -> ContentType.ASCIIDOC;
      case "pipeline" -> ContentType.PIPELINE;
      case "workflow" -> ContentType.WORKFLOW;
      case "metadata" -> ContentType.METADATA;
      case "article", "blog" -> ContentType.MARKDOWN;
      case "plugin" -> ContentType.PLAIN;
      default -> ContentType.AUTO;
    };
  }
}
