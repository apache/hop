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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.lint.registry.RulePackInfo;

/**
 * How a lint run was set up and how it ended, for a report to record next to its findings.
 *
 * <p>A report that lists findings only cannot tell a rule that ran and found nothing from one that
 * was switched off or not installed, which is what someone using it as audit evidence needs to
 * know.
 *
 * @param executionSuccessful whether the run completed; findings do not make it unsuccessful
 * @param exitCode what {@code hop lint} exits with: 1 when the run fails, 0 when it passes
 * @param enabledRules every rule the run evaluated, whether it found anything or not
 * @param rulePacks the installed packs the rules came from
 * @param configuration the settings that decided the findings and the outcome, by name
 */
public record LintInvocation(
    boolean executionSuccessful,
    int exitCode,
    List<CustomLintRule> enabledRules,
    List<RulePackInfo> rulePacks,
    Map<String, Object> configuration) {

  public LintInvocation {
    enabledRules = enabledRules != null ? List.copyOf(enabledRules) : List.of();
    rulePacks = rulePacks != null ? List.copyOf(rulePacks) : List.of();
    configuration =
        configuration != null
            ? Collections.unmodifiableMap(new LinkedHashMap<>(configuration))
            : Map.of();
  }
}
