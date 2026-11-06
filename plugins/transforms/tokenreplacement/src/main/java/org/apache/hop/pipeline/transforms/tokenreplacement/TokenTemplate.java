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

package org.apache.hop.pipeline.transforms.tokenreplacement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.util.Utils;

/**
 * A template split once into fixed text and tokens, so a row only fills in the values. It follows
 * the same rules as {@link TokenReplacingReader}: a token runs from the start marker to the first
 * end marker after it; a token without a value is kept as it is written, markers included; a start
 * marker without an end marker is kept with the rest of the text; values are inserted as they are,
 * never searched for tokens again.
 */
public class TokenTemplate {

  /** The fixed text before each token, and after the last one: one more than there are tokens. */
  private final String[] texts;

  /** For each token: the position of its value in the values of a row, or -1 when there is none. */
  private final int[] valuePositions;

  /** For each token: the token as written in the template, markers included. */
  private final String[] writtenTokens;

  private TokenTemplate(String[] texts, int[] valuePositions, String[] writtenTokens) {
    this.texts = texts;
    this.valuePositions = valuePositions;
    this.writtenTokens = writtenTokens;
  }

  /**
   * Splits a template into fixed text and tokens.
   *
   * @param template the template
   * @param startMarker the text a token starts with, for example "${"
   * @param endMarker the text a token ends with, for example "}"
   * @param valuePositions the position of the value of each token name in the values of a row
   * @return the template, ready to fill in
   */
  public static TokenTemplate parse(
      String template, String startMarker, String endMarker, Map<String, Integer> valuePositions) {
    if (Utils.isEmpty(startMarker) || Utils.isEmpty(endMarker)) {
      throw new IllegalArgumentException("Token start / end marker may not be null or empty");
    }
    List<String> texts = new ArrayList<>();
    List<Integer> positions = new ArrayList<>();
    List<String> written = new ArrayList<>();

    StringBuilder text = new StringBuilder();
    int position = 0;
    while (position < template.length()) {
      int start = template.indexOf(startMarker, position);
      if (start < 0) {
        break;
      }
      int nameStart = start + startMarker.length();
      int end = template.indexOf(endMarker, nameStart);
      if (end < 0) {
        // No end marker: the start marker and the rest are plain text
        break;
      }
      text.append(template, position, start);
      texts.add(text.toString());
      text.setLength(0);

      String name = template.substring(nameStart, end);
      Integer valuePosition = valuePositions.get(name);
      positions.add(valuePosition == null ? -1 : valuePosition);
      written.add(template.substring(start, end + endMarker.length()));

      position = end + endMarker.length();
    }
    text.append(template, position, template.length());
    texts.add(text.toString());

    int[] valuePositionArray = new int[positions.size()];
    for (int i = 0; i < valuePositionArray.length; i++) {
      valuePositionArray[i] = positions.get(i);
    }
    return new TokenTemplate(
        texts.toArray(new String[0]), valuePositionArray, written.toArray(new String[0]));
  }

  /**
   * Fills in the values of one row.
   *
   * @param values the values of the row, by position; a null value keeps the token as written
   * @param output where the result is appended
   */
  public void appendTo(String[] values, StringBuilder output) {
    for (int i = 0; i < valuePositions.length; i++) {
      output.append(texts[i]);
      int valuePosition = valuePositions[i];
      String value = valuePosition < 0 ? null : values[valuePosition];
      output.append(value == null ? writtenTokens[i] : value);
    }
    output.append(texts[texts.length - 1]);
  }

  /** The number of tokens in the template. */
  public int getTokenCount() {
    return valuePositions.length;
  }
}
