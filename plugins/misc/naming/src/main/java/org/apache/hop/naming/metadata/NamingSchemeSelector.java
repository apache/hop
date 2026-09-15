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

package org.apache.hop.naming.metadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * Picks naming schemes for a widget type. Type-specific schemes win; {@link
 * NamingSchemeType#GENERAL} is used only when none of the requested type exist.
 */
public final class NamingSchemeSelector {

  private NamingSchemeSelector() {
    // utility
  }

  /**
   * @param schemes all stored schemes (may be null)
   * @param requested the widget's naming type (null treated as {@link NamingSchemeType#HOP_FIELD})
   * @return matching schemes, never null
   */
  public static List<NamingScheme> matching(
      Iterable<NamingScheme> schemes, NamingSchemeType requested) {
    return matching(
        schemes, requested != null ? requested.getCode() : NamingSchemeType.HOP_FIELD.getCode());
  }

  /**
   * Same as {@link #matching(Iterable, NamingSchemeType)} but compares type <em>codes</em> so
   * plugin kinds (for example {@code dv-hub}) work without an enum constant.
   */
  public static List<NamingScheme> matching(Iterable<NamingScheme> schemes, String requestedCode) {
    String type =
        StringUtils.isNotEmpty(requestedCode)
            ? requestedCode.trim()
            : NamingSchemeType.HOP_FIELD.getCode();
    List<NamingScheme> specific = new ArrayList<>();
    List<NamingScheme> general = new ArrayList<>();
    if (schemes == null) {
      return specific;
    }
    for (NamingScheme scheme : schemes) {
      if (scheme == null) {
        continue;
      }
      String schemeType = StringUtils.trimToEmpty(scheme.getType());
      if (type.equalsIgnoreCase(schemeType)) {
        specific.add(scheme);
      } else if (NamingSchemeType.GENERAL.getCode().equalsIgnoreCase(schemeType)) {
        general.add(scheme);
      }
    }
    if (NamingSchemeType.GENERAL.getCode().equalsIgnoreCase(type) || !specific.isEmpty()) {
      return specific;
    }
    return general;
  }

  /**
   * Resolve the scheme to apply automatically. An explicit name wins; otherwise the unique match
   * from {@link #matching(Iterable, NamingSchemeType)} is used.
   *
   * @param schemes all stored schemes
   * @param requested widget type
   * @param explicitName optional scheme name
   * @return the scheme to apply, or null when none / ambiguous
   */
  public static NamingScheme resolve(
      Iterable<NamingScheme> schemes, NamingSchemeType requested, String explicitName) {
    if (schemes == null) {
      return null;
    }
    if (StringUtils.isNotBlank(explicitName)) {
      String wanted = explicitName.trim();
      for (NamingScheme scheme : schemes) {
        if (scheme != null && wanted.equals(scheme.getName())) {
          return scheme;
        }
      }
      return null;
    }
    List<NamingScheme> match = matching(schemes, requested);
    return match.size() == 1 ? match.get(0) : null;
  }

  public static NamingScheme resolve(
      Iterable<NamingScheme> schemes, String requestedCode, String explicitName) {
    if (StringUtils.isNotBlank(explicitName)) {
      return resolve(schemes, NamingSchemeType.HOP_FIELD, explicitName);
    }
    List<NamingScheme> match = matching(schemes, requestedCode);
    return match.size() == 1 ? match.get(0) : null;
  }
}
