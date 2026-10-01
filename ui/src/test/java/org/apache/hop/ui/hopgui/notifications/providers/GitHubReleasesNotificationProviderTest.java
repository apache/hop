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
package org.apache.hop.ui.hopgui.notifications.providers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/** Unit tests for how a GitHub release is read. */
public class GitHubReleasesNotificationProviderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testAFieldThatIsPresentButNullReadsAsAbsent() throws Exception {
    // GitHub answers with an explicit null rather than leaving the field out: a release made from
    // a tag alone carries "name": null and "body": null. has() is true for those and
    // NullNode.asText() is the four characters "null", which titled the notification "null".
    JsonNode release = MAPPER.readTree("{\"name\":null,\"body\":null,\"tag_name\":\"2.19.0\"}");

    assertNull(GitHubReleasesNotificationProvider.text(release, "name"));
    assertNull(GitHubReleasesNotificationProvider.text(release, "body"));
    assertEquals("2.19.0", GitHubReleasesNotificationProvider.text(release, "tag_name"));
  }

  @Test
  public void testAMissingFieldReadsAsAbsent() throws Exception {
    JsonNode release = MAPPER.readTree("{\"tag_name\":\"2.19.0\"}");

    assertNull(GitHubReleasesNotificationProvider.text(release, "name"));
    assertNull(GitHubReleasesNotificationProvider.text(release, "html_url"));
  }

  @Test
  public void testAValueIsReadAsItself() throws Exception {
    JsonNode release =
        MAPPER.readTree("{\"name\":\"Apache Hop 2.19.0\",\"id\":42,\"draft\":false}");

    assertEquals("Apache Hop 2.19.0", GitHubReleasesNotificationProvider.text(release, "name"));
    // The numeric id identifies a release that was never tagged, and is read as text.
    assertEquals("42", GitHubReleasesNotificationProvider.text(release, "id"));
  }
}
