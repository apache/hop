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

package org.apache.hop.pipeline.transforms.rest;

import jakarta.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.util.Utils;

/**
 * Generic RFC 5988 / RFC 8288 {@code Link} header handling for pagination: find the bracketed URI
 * whose {@code rel=} parameter mentions the {@code next} relation token (similar in spirit to n8n's
 * "response contains next URL" mode, except the pointer often lives in the {@code Link} header—as
 * with GitHub and many other REST APIs—rather than the body).
 *
 * @see <a href="https://www.rfc-editor.org/rfc/rfc5988">RFC 5988 Web Linking</a>
 * @see <a href="https://www.rfc-editor.org/rfc/rfc8288">RFC 8288</a>
 * @see <a href="https://docs.github.com/rest/guides/using-pagination-in-the-rest-api">GitHub
 *     pagination</a>
 */
final class LinkHeaderPaging {

  /**
   * Parameters after closing {@code >} until end of segment, e.g. {@code ; rel=\"next\"; type=}.
   */
  private static final Pattern URI_REF =
      Pattern.compile("^\\s*<([^>]*)>\\s*(.*)\\s*", Pattern.DOTALL);

  private static final Pattern REL_PARAM =
      Pattern.compile(
          "(?:^|[;,])\\s*rel\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^;,\\s]+))",
          Pattern.CASE_INSENSITIVE);

  private LinkHeaderPaging() {}

  /** Collects {@code Link} header values case-insensitively and merges them comma-wise. */
  static String concatenateLinkHeaders(MultivaluedMap<String, ?> headers) {
    if (headers == null) {
      return "";
    }
    StringJoiner joiner = new StringJoiner(", ");
    for (Map.Entry<String, ? extends List<?>> e : headers.entrySet()) {
      if (e.getKey() != null && "link".equalsIgnoreCase(e.getKey()) && e.getValue() != null) {
        for (Object o : e.getValue()) {
          if (o != null) {
            String s = o.toString().trim();
            if (!s.isEmpty()) {
              joiner.add(s);
            }
          }
        }
      }
    }
    return joiner.toString().trim();
  }

  /**
   * @return bracket-URI whose {@code rel} includes the {@code next} token, else null
   */
  static String findFirstUriWithRelNext(MultivaluedMap<String, ?> headers) {
    String concatenated = concatenateLinkHeaders(headers);
    if (Utils.isEmpty(concatenated)) {
      return null;
    }
    for (String segment : splitIntoLinkSegments(concatenated)) {
      Matcher mUri = URI_REF.matcher(segment);
      if (!mUri.matches()) {
        continue;
      }
      String uri = mUri.group(1).trim();
      String tail = mUri.group(2);
      if (!uri.isEmpty() && relationsIncludeNext(tail)) {
        return uri;
      }
    }
    return null;
  }

  /**
   * Split a {@code Link} field value between link-values. Commas appear inside URIs rarely; commas
   * inside {@code <>} are skipped so query strings with commas do not terminate a segment wrongly.
   */
  static List<String> splitIntoLinkSegments(String concatenatedValues) {
    List<String> out = new ArrayList<>();
    boolean inAngles = false;
    int segStart = 0;
    for (int i = 0; i <= concatenatedValues.length(); i++) {
      if (i < concatenatedValues.length()) {
        char c = concatenatedValues.charAt(i);
        if (c == '<') {
          inAngles = true;
        } else if (c == '>') {
          inAngles = false;
        }
      }
      if (i == concatenatedValues.length() || (concatenatedValues.charAt(i) == ',' && !inAngles)) {
        String segment = concatenatedValues.substring(segStart, i).trim();
        if (!segment.isEmpty()) {
          out.add(segment);
        }
        segStart = i + 1;
      }
    }
    return out;
  }

  static boolean relationsIncludeNext(String segmentTail) {
    if (segmentTail == null || segmentTail.isEmpty()) {
      return false;
    }
    Matcher m = REL_PARAM.matcher(segmentTail);
    while (m.find()) {
      String v = nonEmptyGroup(m.group(1), nonEmptyGroup(m.group(2), m.group(3)));
      if (v == null || v.isEmpty()) {
        continue;
      }
      for (String token : v.trim().split("\\s+")) {
        if (!token.isEmpty() && "next".equalsIgnoreCase(token)) {
          return true;
        }
      }
    }
    return false;
  }

  private static String nonEmptyGroup(String first, String second) {
    if (first != null && !first.isEmpty()) {
      return first;
    }
    return second;
  }
}
