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

package org.apache.hop.core.search;

import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.util.HopJaroWinklerDistance;
import org.apache.hop.core.util.StringUtil;

/**
 * A single, reusable string matcher shared by every search box in Hop (global search, file explorer
 * filter, settings filter, the transform/action picker, ...). It is built once from a query and
 * then scores any number of candidate strings.
 *
 * <p>Matching is, in order of decreasing score:
 *
 * <ol>
 *   <li>exact (the whole normalized target equals the term),
 *   <li>word-prefix (a word in the target starts with the term),
 *   <li>substring (the term appears somewhere in the target),
 *   <li>fuzzy (Jaro-Winkler similarity above a threshold) - only when fuzzy matching is enabled.
 * </ol>
 *
 * <p>The query is normalized (diacritics removed, optionally lower-cased, whitespace collapsed) and
 * split into terms on whitespace and commas; <b>all</b> terms must match (AND). Alternatively a
 * regular-expression mode does a partial regex match.
 *
 * <p>{@link #score(String)} returns a value in {@code [0,1]} ({@code 0} means "no match") which
 * callers can use to rank results; {@link #matches(String)} is the boolean shortcut.
 */
public class SearchMatcher {

  /** Default Jaro-Winkler similarity above which a fuzzy match is accepted. */
  public static final double DEFAULT_FUZZY_THRESHOLD = 0.86;

  /** Fuzzy matching is only attempted for terms at least this long (avoids noise on tiny terms). */
  private static final int MIN_FUZZY_TERM_LENGTH = 3;

  /**
   * A fuzzy match is only considered when the shorter of the two strings is at least this fraction
   * of the longer one. Without this a long query (e.g. "tableinput") would fuzzily latch onto a
   * much shorter candidate (e.g. the keyword "table") thanks to the Jaro-Winkler common-prefix
   * bonus, which is too lenient: a genuine typo is roughly the same length as its target.
   */
  private static final double MIN_FUZZY_LENGTH_RATIO = 0.6;

  private static final double SCORE_EXACT = 1.0;
  private static final double SCORE_WORD = 0.97;
  private static final double SCORE_PREFIX = 0.92;
  private static final double SCORE_SUBSTRING = 0.8;
  private static final double FUZZY_WEIGHT = 0.7;

  private final boolean caseSensitive;
  private final boolean regex;
  private final boolean fuzzy;
  private final double fuzzyThreshold;
  private final String[] terms;
  private final Pattern pattern;

  public SearchMatcher(String query, boolean caseSensitive, boolean regEx, boolean fuzzy) {
    this(query, caseSensitive, regEx, fuzzy, DEFAULT_FUZZY_THRESHOLD);
  }

  public SearchMatcher(
      String query, boolean caseSensitive, boolean regEx, boolean fuzzy, double fuzzyThreshold) {
    this.caseSensitive = caseSensitive;
    this.regex = regEx;
    this.fuzzy = fuzzy && !regEx;
    this.fuzzyThreshold = fuzzyThreshold;

    if (regEx) {
      Pattern compiled = null;
      if (StringUtils.isNotEmpty(query)) {
        try {
          compiled = Pattern.compile(query, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
        } catch (Exception e) {
          // An incomplete/invalid expression simply matches nothing instead of throwing.
          compiled = null;
        }
      }
      this.pattern = compiled;
      this.terms = new String[0];
    } else {
      this.pattern = null;
      String normalized = normalize(query, caseSensitive);
      this.terms = normalized.isEmpty() ? new String[0] : normalized.split(" ");
    }
  }

  /**
   * Normalize a string for matching: remove diacritical marks, optionally lower-case, turn commas
   * into spaces and collapse runs of whitespace.
   */
  public static String normalize(String string, boolean caseSensitive) {
    if (StringUtils.isEmpty(string)) {
      return "";
    }
    String normalized = StringUtil.removeDiacriticalMarks(string);
    if (!caseSensitive) {
      normalized = normalized.toLowerCase(Locale.ROOT);
    }
    return normalized.replace(',', ' ').replaceAll("\\s+", " ").trim();
  }

  /** Whether the given target matches at all. */
  public boolean matches(String target) {
    return score(target) > 0.0;
  }

  /**
   * Score a candidate string against the query.
   *
   * @param target the candidate
   * @return a relevance score in {@code [0,1]}; {@code 0} means no match
   */
  public double score(String target) {
    // Regular expression mode: a partial match counts. An invalid/empty pattern matches nothing.
    if (regex) {
      return pattern != null && target != null && pattern.matcher(target).find() ? 1.0 : 0.0;
    }

    // An empty query matches everything non-empty (keeps the old "match all" behavior).
    if (terms.length == 0) {
      return StringUtils.isNotEmpty(target) ? 1.0 : 0.0;
    }
    if (StringUtils.isEmpty(target)) {
      return 0.0;
    }

    String normalizedTarget = normalize(target, caseSensitive);
    String[] words = normalizedTarget.split(" ");

    double total = 0.0;
    for (String term : terms) {
      double termScore = scoreTerm(term, normalizedTarget, words);
      if (termScore <= 0.0) {
        // AND semantics: every term must match.
        return 0.0;
      }
      total += termScore;
    }
    return total / terms.length;
  }

  private double scoreTerm(String term, String target, String[] words) {
    if (target.equals(term)) {
      return SCORE_EXACT;
    }
    if (target.contains(term)) {
      for (String word : words) {
        if (word.equals(term)) {
          return SCORE_WORD;
        }
      }
      for (String word : words) {
        if (word.startsWith(term)) {
          return SCORE_PREFIX;
        }
      }
      return SCORE_SUBSTRING;
    }
    if (fuzzy && term.length() >= MIN_FUZZY_TERM_LENGTH) {
      double best = similarity(term, target);
      for (String word : words) {
        best = Math.max(best, similarity(term, word));
      }
      if (best >= fuzzyThreshold) {
        return best * FUZZY_WEIGHT;
      }
    }
    return 0.0;
  }

  /**
   * Jaro-Winkler similarity, but {@code 0} when the two strings differ too much in length to be a
   * plausible typo of one another (see {@link #MIN_FUZZY_LENGTH_RATIO}).
   */
  private static double similarity(String a, String b) {
    if (StringUtils.isEmpty(a) || StringUtils.isEmpty(b)) {
      return 0.0;
    }
    int shorter = Math.min(a.length(), b.length());
    int longer = Math.max(a.length(), b.length());
    if ((double) shorter / longer < MIN_FUZZY_LENGTH_RATIO) {
      return 0.0;
    }
    HopJaroWinklerDistance distance = new HopJaroWinklerDistance();
    distance.apply(a, b);
    Double jw = distance.getJaroWinklerDistance();
    return jw == null ? 0.0 : jw;
  }
}
