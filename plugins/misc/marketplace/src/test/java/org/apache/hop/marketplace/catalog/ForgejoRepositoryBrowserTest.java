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

package org.apache.hop.marketplace.catalog;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.marketplace.catalog.ForgejoRepositoryBrowser.Candidate;
import org.junit.jupiter.api.Test;

class ForgejoRepositoryBrowserTest {

  private static final String PACKAGES_JSON =
      """
      [
        {
          "id": 1,
          "type": "maven",
          "name": "com.acme:acme-parser",
          "version": "2026.09",
          "created_at": "2026-07-20T10:00:00Z"
        },
        {
          "id": 2,
          "type": "maven",
          "name": "com.acme:acme-parser",
          "version": "2026.06",
          "created_at": "2026-04-01T10:00:00Z"
        },
        {
          "id": 3,
          "type": "container",
          "name": "some/image",
          "version": "1.0"
        },
        {
          "id": 4,
          "type": "maven",
          "name": "no-colon-here",
          "version": "1.0"
        }
      ]
      """;

  @Test
  void parsesMavenPackagesOnly() throws Exception {
    List<Candidate> candidates = ForgejoRepositoryBrowser.parsePackagesPage(PACKAGES_JSON);
    // container package and the colon-less name are both dropped
    assertEquals(2, candidates.size());
    assertEquals("com.acme", candidates.get(0).groupId());
    assertEquals("acme-parser", candidates.get(0).artifactId());
    assertEquals("2026.09", candidates.get(0).version());
    assertEquals("2026-07-20T10:00:00Z", candidates.get(0).createdAt());
  }

  @Test
  void emptyPageIsEmptyList() throws Exception {
    assertTrue(ForgejoRepositoryBrowser.parsePackagesPage("[]").isEmpty());
    assertTrue(ForgejoRepositoryBrowser.parsePackagesPage("").isEmpty());
    assertTrue(ForgejoRepositoryBrowser.parsePackagesPage(null).isEmpty());
  }

  @Test
  void splitsGroupAndArtifact() {
    assertArrayEquals(
        new String[] {"com.acme", "acme-parser"},
        ForgejoRepositoryBrowser.splitPackageName("com.acme:acme-parser"));
    assertArrayEquals(
        new String[] {"org.apache.hop", "hop-tech-parquet"},
        ForgejoRepositoryBrowser.splitPackageName(" org.apache.hop:hop-tech-parquet "));
    assertNull(ForgejoRepositoryBrowser.splitPackageName("no-colon"));
    assertNull(ForgejoRepositoryBrowser.splitPackageName(":leading"));
    assertNull(ForgejoRepositoryBrowser.splitPackageName("trailing:"));
    assertNull(ForgejoRepositoryBrowser.splitPackageName(""));
    assertNull(ForgejoRepositoryBrowser.splitPackageName(null));
  }

  @Test
  void findsPluginZipAmongFiles() throws Exception {
    String files =
        """
        [
          {"name": "acme-parser-2026.09.pom", "size": 100},
          {"name": "acme-parser-2026.09.jar", "size": 200},
          {"name": "acme-parser-2026.09.zip", "size": 300}
        ]
        """;
    assertEquals("acme-parser-2026.09.zip", ForgejoRepositoryBrowser.extractZipFileName(files));
  }

  @Test
  void jarOnlyArtifactHasNoZip() throws Exception {
    String files =
        """
        [
          {"name": "acme-shared-2026.09.pom"},
          {"name": "acme-shared-2026.09.jar"}
        ]
        """;
    // Shared libraries must not be offered as installable plugins.
    assertNull(ForgejoRepositoryBrowser.extractZipFileName(files));
    assertNull(ForgejoRepositoryBrowser.extractZipFileName("[]"));
    assertNull(ForgejoRepositoryBrowser.extractZipFileName(null));
  }

  @Test
  void derivesHostAndOwnerFromRegistryUrl() {
    String url = "https://forge.example.org/api/packages/acme/maven";
    assertEquals("https://forge.example.org", ForgejoRepositoryBrowser.extractApiBase(url));
    assertEquals("acme", ForgejoRepositoryBrowser.extractOwner(url));

    // trailing slash and a bare owner endpoint both work
    assertEquals(
        "acme",
        ForgejoRepositoryBrowser.extractOwner(
            "https://forge.example.org/api/packages/acme/maven/"));
    assertEquals(
        "acme", ForgejoRepositoryBrowser.extractOwner("https://git.example.org/api/packages/acme"));
    assertEquals(
        "https://git.example.org",
        ForgejoRepositoryBrowser.extractApiBase("https://git.example.org/api/packages/acme"));
  }

  @Test
  void nonForgejoUrlYieldsNothing() {
    String nexus = "https://repository.apache.org/content/groups/public/";
    assertNull(ForgejoRepositoryBrowser.extractApiBase(nexus));
    assertNull(ForgejoRepositoryBrowser.extractOwner(nexus));
    assertNull(ForgejoRepositoryBrowser.extractApiBase(null));
    assertNull(ForgejoRepositoryBrowser.extractOwner(null));
  }
}
