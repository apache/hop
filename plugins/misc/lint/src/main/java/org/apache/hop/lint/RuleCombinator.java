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

package org.apache.hop.lint;

/**
 * How the clauses of a composed rule combine into one verdict.
 *
 * <p>Both are stated as "when does this rule report", so that reading a rule out loud matches what
 * it does: {@code allOf} reports when every clause is broken, {@code anyOf} when one is.
 */
public enum RuleCombinator {
  /** Report only when every clause is violated. The YAML key is {@code allOf}. */
  ALL_OF("allOf"),

  /** Report when at least one clause is violated. The YAML key is {@code anyOf}. */
  ANY_OF("anyOf");

  private final String yamlKey;

  RuleCombinator(String yamlKey) {
    this.yamlKey = yamlKey;
  }

  public String getYamlKey() {
    return yamlKey;
  }

  /**
   * The combinator a YAML key names.
   *
   * @param key the key read from the rule
   * @return the combinator, or null when the key names neither
   */
  public static RuleCombinator fromYamlKey(String key) {
    for (RuleCombinator combinator : values()) {
      if (combinator.yamlKey.equalsIgnoreCase(key)) {
        return combinator;
      }
    }
    return null;
  }
}
