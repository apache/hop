/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.marketplace.resolve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class UrlTemplateTest {

  private static final MavenCoordinates COORDS =
      new MavenCoordinates("com.acme.hop", "acme-parser", "2026.09");

  @Test
  void expandsReleaseAssetTemplate() throws Exception {
    String template =
        "https://forge.example.org/acme/dist/releases/download/v${version}/${artifactId}-${version}.zip";
    assertEquals(
        "https://forge.example.org/acme/dist/releases/download/v2026.09/acme-parser-2026.09.zip",
        MavenRepositoryClient.expandUrlTemplate(template, COORDS));
  }

  @Test
  void expandsGroupPlaceholders() throws Exception {
    assertEquals(
        "https://cdn.example.org/com/acme/hop/acme-parser/2026.09/acme-parser-2026.09.zip",
        MavenRepositoryClient.expandUrlTemplate(
            "https://cdn.example.org/${groupPath}/${artifactId}/${version}/${artifactId}-${version}.zip",
            COORDS));
    assertEquals(
        "https://cdn.example.org/com.acme.hop/acme-parser.zip",
        MavenRepositoryClient.expandUrlTemplate(
            "https://cdn.example.org/${groupId}/${artifactId}.zip", COORDS));
  }

  @Test
  void templateWithoutPlaceholdersIsUsedAsIs() throws Exception {
    assertEquals(
        "https://example.org/fixed.zip",
        MavenRepositoryClient.expandUrlTemplate("https://example.org/fixed.zip", COORDS));
  }

  @Test
  void unknownPlaceholderIsRejected() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                MavenRepositoryClient.expandUrlTemplate(
                    "https://example.org/${classifier}/${artifactId}.zip", COORDS));
    // A silently unexpanded placeholder would produce a 404 that is hard to diagnose.
    assertTrue(e.getMessage().contains("Unresolved placeholder"));
  }

  @Test
  void blankTemplateIsRejected() {
    assertThrows(HopException.class, () -> MavenRepositoryClient.expandUrlTemplate("", COORDS));
    assertThrows(HopException.class, () -> MavenRepositoryClient.expandUrlTemplate(null, COORDS));
  }
}
