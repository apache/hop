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

import java.util.regex.Pattern;

/** Splits AsciiDoc on {@code =} headings and builds a section tree. */
public final class AsciiDocDocumentParser implements DocumentParser {

  private static final Pattern HEADING = Pattern.compile("(?m)^(=+)\\s+(.+)$");

  @Override
  public ContentType getContentType() {
    return ContentType.ASCIIDOC;
  }

  @Override
  public DocumentNode parse(String text) {
    return HeadingParserSupport.parse(text, HEADING, String::length);
  }
}
