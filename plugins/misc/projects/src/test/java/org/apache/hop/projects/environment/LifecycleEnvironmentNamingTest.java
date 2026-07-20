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

package org.apache.hop.projects.environment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LifecycleEnvironmentNamingTest {

  private Map<String, String> knownSuffixes;

  @BeforeEach
  void setUp() {
    knownSuffixes =
        LifecycleEnvironmentNaming.knownPurposeSuffixes(
            "Development",
            "Testing",
            "Acceptance",
            "Production",
            "Continuous Integration",
            "Common Build");
  }

  @Test
  void suggestProjectOnlyWhenPurposeEmpty() {
    assertEquals(
        "my-project",
        LifecycleEnvironmentNaming.suggestEnvironmentName("my-project", "", knownSuffixes));
    assertEquals(
        "my-project",
        LifecycleEnvironmentNaming.suggestEnvironmentName("my-project", null, knownSuffixes));
    assertEquals(
        "my-project",
        LifecycleEnvironmentNaming.suggestEnvironmentName("my-project", "  ", knownSuffixes));
  }

  @Test
  void suggestEmptyWhenProjectBlank() {
    assertEquals(
        "", LifecycleEnvironmentNaming.suggestEnvironmentName(null, "Development", knownSuffixes));
    assertEquals(
        "", LifecycleEnvironmentNaming.suggestEnvironmentName("", "Development", knownSuffixes));
    assertEquals(
        "", LifecycleEnvironmentNaming.suggestEnvironmentName("  ", "Development", knownSuffixes));
  }

  @Test
  void suggestKnownPurposesUseEnglishSuffixes() {
    assertEquals(
        "sales-development",
        LifecycleEnvironmentNaming.suggestEnvironmentName("sales", "Development", knownSuffixes));
    assertEquals(
        "sales-test",
        LifecycleEnvironmentNaming.suggestEnvironmentName("sales", "Testing", knownSuffixes));
    assertEquals(
        "sales-acceptance",
        LifecycleEnvironmentNaming.suggestEnvironmentName("sales", "Acceptance", knownSuffixes));
    assertEquals(
        "sales-production",
        LifecycleEnvironmentNaming.suggestEnvironmentName("sales", "Production", knownSuffixes));
    assertEquals(
        "sales-continuous-integration",
        LifecycleEnvironmentNaming.suggestEnvironmentName(
            "sales", "Continuous Integration", knownSuffixes));
    assertEquals(
        "sales-common-build",
        LifecycleEnvironmentNaming.suggestEnvironmentName("sales", "Common Build", knownSuffixes));
  }

  @Test
  void knownLocalizedLabelsMapToEnglishSuffixes() {
    Map<String, String> italian =
        LifecycleEnvironmentNaming.knownPurposeSuffixes(
            "Sviluppo",
            "Testing",
            "Accettazione",
            "Produzione",
            "Continuous Integration",
            "Common Build");

    assertEquals(
        "app-development",
        LifecycleEnvironmentNaming.suggestEnvironmentName("app", "Sviluppo", italian));
    assertEquals(
        "app-test", LifecycleEnvironmentNaming.suggestEnvironmentName("app", "Testing", italian));
    assertEquals(
        "app-acceptance",
        LifecycleEnvironmentNaming.suggestEnvironmentName("app", "Accettazione", italian));
    assertEquals(
        "app-production",
        LifecycleEnvironmentNaming.suggestEnvironmentName("app", "Produzione", italian));
  }

  @Test
  void freeTextPurposeIsSanitized() {
    assertEquals(
        "my-project-staging",
        LifecycleEnvironmentNaming.suggestEnvironmentName("my-project", "Staging", knownSuffixes));
    assertEquals(
        "my-project-pre-prod",
        LifecycleEnvironmentNaming.suggestEnvironmentName("my-project", "Pre Prod", knownSuffixes));
    assertEquals(
        "my-project-qa.1",
        LifecycleEnvironmentNaming.suggestEnvironmentName("my-project", "QA.1", knownSuffixes));
  }

  @Test
  void purposeToSuffixKnownAndFreeText() {
    assertEquals(
        LifecycleEnvironmentNaming.SUFFIX_TEST,
        LifecycleEnvironmentNaming.purposeToSuffix("Testing", knownSuffixes));
    assertEquals(
        LifecycleEnvironmentNaming.SUFFIX_DEVELOPMENT,
        LifecycleEnvironmentNaming.purposeToSuffix("Development", knownSuffixes));
    assertEquals(
        "custom-env", LifecycleEnvironmentNaming.purposeToSuffix("Custom Env", knownSuffixes));
    assertEquals("", LifecycleEnvironmentNaming.purposeToSuffix(null, knownSuffixes));
    assertEquals("", LifecycleEnvironmentNaming.purposeToSuffix("", knownSuffixes));
  }

  @Test
  void sanitizePurposeSuffix() {
    assertEquals("hello-world", LifecycleEnvironmentNaming.sanitizePurposeSuffix(" Hello  World "));
    assertEquals("a-b-c", LifecycleEnvironmentNaming.sanitizePurposeSuffix("A/B\\C"));
    assertEquals("", LifecycleEnvironmentNaming.sanitizePurposeSuffix("@@@"));
    assertEquals("", LifecycleEnvironmentNaming.sanitizePurposeSuffix(null));
  }

  @Test
  void suggestWithSuffixDirectly() {
    assertEquals(
        "p-development",
        LifecycleEnvironmentNaming.suggestEnvironmentName(
            "p", LifecycleEnvironmentNaming.SUFFIX_DEVELOPMENT));
    assertEquals("p", LifecycleEnvironmentNaming.suggestEnvironmentName("p", ""));
  }
}
