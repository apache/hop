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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.junit.jupiter.api.Test;

/** URL derivation, AQL query building and response parsing for the Artifactory browser. */
class JfrogRepositoryBrowserTest {

  private static final String AQL_RESULTS =
      """
      {
        "results": [
          {
            "repo": "hop-plugins",
            "path": "com/acme/hop/acme-parser/2026.09",
            "name": "acme-parser-2026.09.zip",
            "created": "2026-07-20T10:00:00.000Z",
            "modified": "2026-07-21T10:00:00.000Z"
          },
          {
            "repo": "hop-plugins",
            "path": "com/acme/hop/acme-parser/2026.06",
            "name": "acme-parser-2026.06.zip",
            "created": "2026-04-01T10:00:00.000Z",
            "modified": "2026-04-01T10:00:00.000Z"
          },
          {
            "repo": "hop-plugins",
            "path": "com/acme/hop/acme-lib/1.0.0",
            "name": "acme-lib-1.0.0.jar",
            "modified": "2026-08-01T10:00:00.000Z"
          },
          {
            "repo": "hop-plugins",
            "path": "toplevel",
            "name": "stray.zip",
            "modified": "2026-08-01T10:00:00.000Z"
          }
        ],
        "range": {"start_pos": 0, "end_pos": 4, "total": 4}
      }
      """;

  private static MarketplaceRepository repo() {
    return new MarketplaceRepository(
        "artifactory", "https://acme.jfrog.io/artifactory/hop-plugins/");
  }

  @Test
  void derivesBaseAndRepositoryKey() {
    String cloud = "https://acme.jfrog.io/artifactory/hop-plugins/";
    assertEquals(
        "https://acme.jfrog.io/artifactory", JfrogRepositoryBrowser.extractArtifactoryBase(cloud));
    assertEquals("hop-plugins", JfrogRepositoryBrowser.extractRepoKey(cloud));

    String selfHosted = "https://artifactory.example.com/artifactory/hop-plugins-local";
    assertEquals(
        "https://artifactory.example.com/artifactory",
        JfrogRepositoryBrowser.extractArtifactoryBase(selfHosted));
    assertEquals("hop-plugins-local", JfrogRepositoryBrowser.extractRepoKey(selfHosted));
  }

  @Test
  void nonArtifactoryUrlsDeriveNothing() {
    String nexus = "https://nexus.example.org/repository/hop-plugins/";
    assertNull(JfrogRepositoryBrowser.extractArtifactoryBase(nexus));
    assertNull(JfrogRepositoryBrowser.extractRepoKey(nexus));
    assertNull(JfrogRepositoryBrowser.extractArtifactoryBase(null));
    assertNull(JfrogRepositoryBrowser.extractRepoKey(""));
  }

  @Test
  void aqlQueryCoversRepositoryExtensionAndFilters() {
    String plain = JfrogRepositoryBrowser.buildAqlQuery("hop-plugins", null, null);
    assertTrue(plain.startsWith("items.find("), plain);
    assertTrue(plain.contains("\"repo\":\"hop-plugins\""), plain);
    assertTrue(plain.contains("\"$match\":\"*.zip\""), plain);
    // No path criterion without a groupIdFilter; "path" itself is always in the include list.
    assertFalse(plain.contains("\"path\":{"), plain);
    assertTrue(plain.contains(".limit(" + JfrogRepositoryBrowser.AQL_LIMIT + ")"), plain);
    // Artifactory OSS rejects any query carrying a sort, so there must not be one.
    assertFalse(plain.contains(".sort("), plain);

    String filtered = JfrogRepositoryBrowser.buildAqlQuery("hop-plugins", "com.acme.hop", "parser");
    assertTrue(filtered.contains("\"path\":{\"$match\":\"com/acme/hop/*\"}"), filtered);
    assertTrue(filtered.contains("\"$match\":\"*parser*.zip\""), filtered);
  }

  @Test
  void aqlQueryEscapesConfiguredText() {
    // An unescaped quote would close the JSON string and let the rest be read as query syntax.
    String query = JfrogRepositoryBrowser.buildAqlQuery("repo", null, "a\"b\\c");
    assertTrue(query.contains("\\\"") && query.contains("\\\\"), query);
    assertEquals("\"a\\\"b\"", JfrogRepositoryBrowser.jsonString("a\"b"));
    assertEquals("\"a\\\\b\"", JfrogRepositoryBrowser.jsonString("a\\b"));
    assertEquals("\"a\\u0001b\"", JfrogRepositoryBrowser.jsonString("a" + (char) 1 + "b"));
  }

  @Test
  void aqlResultsBecomeCoordinates() throws Exception {
    List<OptionalPluginInfo> found = JfrogRepositoryBrowser.parseAqlResults(AQL_RESULTS, repo());

    // The jar row and the row that is not Maven layout are both dropped.
    assertEquals(2, found.size());
    OptionalPluginInfo first = found.get(0);
    assertEquals("com.acme.hop", first.getGroupId());
    assertEquals("acme-parser", first.getArtifactId());
    assertEquals("2026.09", first.getVersion());
    assertEquals(
        "com/acme/hop/acme-parser/2026.09/acme-parser-2026.09.zip", first.getInstallPath());
    assertEquals("2026-07-21T10:00:00.000Z", first.getLastUpdated());
    assertEquals("artifactory", first.getSource());
  }

  @Test
  void onlyTheNewestVersionPerArtifactIsListed() throws Exception {
    List<OptionalPluginInfo> out =
        JfrogRepositoryBrowser.newestPerArtifact(
            JfrogRepositoryBrowser.parseAqlResults(AQL_RESULTS, repo()), repo());
    assertEquals(1, out.size());
    assertEquals("2026.09", out.get(0).getVersion());
  }

  @Test
  void emptyAndMalformedResponsesAreEmptyLists() throws Exception {
    assertTrue(JfrogRepositoryBrowser.parseAqlResults("{\"results\":[]}", repo()).isEmpty());
    assertTrue(JfrogRepositoryBrowser.parseAqlResults("{}", repo()).isEmpty());
    assertTrue(JfrogRepositoryBrowser.parseAqlResults("", repo()).isEmpty());
    assertTrue(JfrogRepositoryBrowser.parseAqlResults(null, repo()).isEmpty());
  }

  @Test
  void snapshotVersionComesFromTheFolderNotTheFileName() {
    // Artifactory stores unique snapshots, but only the folder version resolves for download.
    OptionalPluginInfo info =
        JfrogRepositoryBrowser.toPluginInfo(
            "com/acme/hop/acme-parser/1.0.0-SNAPSHOT",
            "acme-parser-1.0.0-20260101.120000-1.zip",
            "2026-01-01T12:00:00.000Z",
            repo());
    assertEquals("1.0.0-SNAPSHOT", info.getVersion());
    assertEquals(
        "com/acme/hop/acme-parser/1.0.0-SNAPSHOT/acme-parser-1.0.0-20260101.120000-1.zip",
        info.getInstallPath());
  }

  @Test
  void snapshotsAreDroppedWhenTheRepositoryHidesThem() {
    MarketplaceRepository repo = repo();
    repo.setIncludeSnapshots(false);
    assertNull(
        JfrogRepositoryBrowser.toPluginInfo(
            "com/acme/hop/acme-parser/1.0.0-SNAPSHOT",
            "acme-parser-1.0.0-SNAPSHOT.zip",
            null,
            repo));
    assertEquals(
        "1.0.0",
        JfrogRepositoryBrowser.toPluginInfo(
                "com/acme/hop/acme-parser/1.0.0", "acme-parser-1.0.0.zip", null, repo)
            .getVersion());
  }

  @Test
  void groupIdFilterIsEnforcedOnResults() {
    // AQL matches the path as a prefix, so a nested group can come back and has to be rejected.
    MarketplaceRepository repo = repo();
    repo.setGroupIdFilter("com.acme.hop");
    assertNull(
        JfrogRepositoryBrowser.toPluginInfo(
            "com/acme/hop/nested/acme-parser/1.0.0", "acme-parser-1.0.0.zip", null, repo));
    assertEquals(
        "com.acme.hop",
        JfrogRepositoryBrowser.toPluginInfo(
                "com/acme/hop/acme-parser/1.0.0", "acme-parser-1.0.0.zip", null, repo)
            .getGroupId());
  }

  @Test
  void pathsThatAreNotMavenLayoutAreSkipped() {
    assertNull(JfrogRepositoryBrowser.toPluginInfo("acme/1.0.0", "acme-1.0.0.zip", null, repo()));
    assertNull(JfrogRepositoryBrowser.toPluginInfo(".", "acme.zip", null, repo()));
    assertNull(JfrogRepositoryBrowser.toPluginInfo("", "acme.zip", null, repo()));
    assertNull(JfrogRepositoryBrowser.toPluginInfo("com/acme/hop/acme/1.0.0", null, null, repo()));
  }

  @Test
  void folderInfoSplitsChildrenByKind() throws Exception {
    JfrogRepositoryBrowser.Folder folder =
        JfrogRepositoryBrowser.parseFolder(
            """
            {
              "repo": "hop-plugins",
              "path": "/com/acme/hop/acme-parser/2026.09",
              "lastModified": "2026-07-21T10:00:00.000Z",
              "children": [
                {"uri": "/acme-parser-2026.09.zip", "folder": false},
                {"uri": "/acme-parser-2026.09.zip.sha1", "folder": false},
                {"uri": "/acme-parser-2026.09.pom", "folder": false},
                {"uri": "/nested", "folder": true}
              ]
            }
            """);
    assertEquals(List.of("nested"), folder.folders());
    assertEquals(3, folder.files().size());
    assertEquals("2026-07-21T10:00:00.000Z", folder.lastModified());
    assertEquals("acme-parser-2026.09.zip", JfrogRepositoryBrowser.firstPluginZip(folder.files()));
  }

  @Test
  void checksumSidecarsAreNotPluginZips() {
    assertFalse(JfrogRepositoryBrowser.isPluginZip("acme-1.0.0.zip.sha1"));
    assertFalse(JfrogRepositoryBrowser.isPluginZip("acme-1.0.0.jar"));
    assertFalse(JfrogRepositoryBrowser.isPluginZip(null));
    assertTrue(JfrogRepositoryBrowser.isPluginZip("acme-1.0.0.ZIP"));
    assertNull(JfrogRepositoryBrowser.firstPluginZip(List.of("a.pom", "a.zip.md5")));
  }

  @Test
  void folderWithoutChildrenIsEmptyRatherThanAFailure() throws Exception {
    JfrogRepositoryBrowser.Folder folder = JfrogRepositoryBrowser.parseFolder("{}");
    assertTrue(folder.folders().isEmpty());
    assertTrue(folder.files().isEmpty());
    assertNull(folder.lastModified());
  }

  @Test
  void repositoryPathSegmentsArePercentEncoded() {
    // A path segment is not a form field: a space is %20 there, never +.
    assertEquals(
        "hop%20plugins/com/acme", JfrogRepositoryBrowser.encodePath("hop plugins/com/acme"));
    assertEquals("com/acme", JfrogRepositoryBrowser.encodePath("/com/acme/"));
  }
}
