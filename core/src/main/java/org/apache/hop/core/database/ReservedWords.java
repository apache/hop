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

package org.apache.hop.core.database;

import java.util.HashSet;
import java.util.Set;

/**
 * An immutable, case-insensitive set of reserved words with O(1) lookups.
 *
 * <p>Two words match exactly when {@link String#equalsIgnoreCase(String)} says they do, which is
 * how reserved words were matched before ({@code Const.indexOfString}). Every code point is folded
 * to {@code toLowerCase(toUpperCase(cp))}; that is the same per-character test {@code
 * equalsIgnoreCase} applies, so the folded forms are equal if and only if the words are. A
 * locale-sensitive {@code String.toUpperCase()} is deliberately not used: it can change the length
 * of a word (ß becomes SS) and depends on the default locale (Turkish dotless i).
 */
public final class ReservedWords {

  public static final ReservedWords EMPTY = new ReservedWords(Set.of());

  private final Set<String> folded;

  private ReservedWords(Set<String> folded) {
    this.folded = folded;
  }

  /**
   * @param words the reserved words, may be null or contain nulls (which are ignored)
   * @return a lookup over these words
   */
  public static ReservedWords of(String[] words) {
    if (words == null || words.length == 0) {
      return EMPTY;
    }
    Set<String> set = new HashSet<>(words.length * 2);
    for (String word : words) {
      if (word != null) {
        set.add(fold(word));
      }
    }
    return new ReservedWords(set);
  }

  /**
   * @param word the word to look up, may be null
   * @return true if the word is in this set, ignoring case
   */
  public boolean contains(String word) {
    if (word == null || folded.isEmpty()) {
      return false;
    }
    return folded.contains(fold(word));
  }

  public int size() {
    return folded.size();
  }

  static String fold(String word) {
    int length = word.length();
    int i = 0;
    // Fast path: lower-case ASCII (the common case for column names) is already folded.
    while (i < length) {
      char c = word.charAt(i);
      if (c >= 0x80 || (c >= 'A' && c <= 'Z')) {
        break;
      }
      i++;
    }
    if (i == length) {
      return word;
    }
    StringBuilder sb = new StringBuilder(length);
    sb.append(word, 0, i);
    while (i < length) {
      int cp = word.codePointAt(i);
      sb.appendCodePoint(Character.toLowerCase(Character.toUpperCase(cp)));
      i += Character.charCount(cp);
    }
    return sb.toString();
  }
}
