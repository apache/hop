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

package org.apache.hop.naming.engine;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingSchemeSelector;

/** Fixed-point check: a name is valid when applying the scheme does not change it. */
public final class NamingSchemeValidator {

  private NamingSchemeValidator() {
    // utility
  }

  /**
   * @param value current name
   * @param typeCode naming kind code
   * @param schemes all project schemes
   * @return empty if valid or skipped; otherwise one finding per scheme that would rewrite
   */
  public static List<Finding> validate(
      String value, String typeCode, Iterable<NamingScheme> schemes) {
    List<Finding> findings = new ArrayList<>();
    if (NamingEngine.shouldSkip(value)) {
      return findings;
    }
    List<NamingScheme> applicable = NamingSchemeSelector.matching(schemes, typeCode);
    if (applicable.isEmpty()) {
      Finding none = new Finding();
      none.severity = Severity.WARNING;
      none.typeCode = typeCode;
      none.actual = value;
      none.message = "No naming scheme of type '" + typeCode + "' (and no General scheme)";
      findings.add(none);
      return findings;
    }
    for (NamingScheme scheme : applicable) {
      String expected = NamingEngine.apply(scheme, value);
      if (expected != null && !expected.equals(value)) {
        Finding finding = new Finding();
        finding.severity = Severity.ERROR;
        finding.typeCode = typeCode;
        finding.schemeName = scheme.getName();
        finding.actual = value;
        finding.expected = expected;
        finding.message =
            "Name '"
                + value
                + "' does not match scheme '"
                + scheme.getName()
                + "' (expected '"
                + expected
                + "')";
        findings.add(finding);
      }
    }
    return findings;
  }

  public enum Severity {
    WARNING,
    ERROR
  }

  @Getter
  @lombok.Setter
  public static class Finding {
    private Severity severity = Severity.ERROR;
    private String location;
    private String fieldPath;
    private String typeCode;
    private String schemeName;
    private String actual;
    private String expected;
    private String message;
  }
}
