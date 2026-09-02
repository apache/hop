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

package org.apache.hop.ui.hopgui.notifications.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.hop.core.util.JsonUtil;
import org.junit.Test;

/** Unit tests for NotificationSourceConfig JSON serialization/deserialization and getPluginId. */
public class NotificationSourceConfigTest {

  @Test
  public void testJsonRoundTrip_githubSource() throws Exception {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId("github-apache-hop");
    source.setName("Apache Hop Releases");
    source.setType(NotificationSourceConfig.SourceType.GITHUB_RELEASES);
    source.setEnabled(true);
    source.setGithubOwner("apache");
    source.setGithubRepo("hop");
    source.setGithubIncludePrereleases(false);
    source.setPollIntervalMinutes("60");
    source.setColor("#FF5733");

    ObjectMapper mapper = JsonUtil.jsonMapper();
    String json = mapper.writeValueAsString(source);
    NotificationSourceConfig restored = mapper.readValue(json, NotificationSourceConfig.class);

    assertEquals(source.getId(), restored.getId());
    assertEquals(source.getName(), restored.getName());
    assertEquals(source.getType(), restored.getType());
    assertEquals(source.isEnabled(), restored.isEnabled());
    assertEquals(source.getGithubOwner(), restored.getGithubOwner());
    assertEquals(source.getGithubRepo(), restored.getGithubRepo());
    assertEquals(source.getPollIntervalMinutes(), restored.getPollIntervalMinutes());
    assertEquals(source.getColor(), restored.getColor());
  }

  @Test
  public void testJsonRoundTrip_rssSource() throws Exception {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId("rss-1");
    source.setName("My Feed");
    source.setType(NotificationSourceConfig.SourceType.RSS_FEED);
    source.setEnabled(true);
    source.setRssUrl("https://example.com/feed.xml");
    source.setPollIntervalMinutes("30");

    ObjectMapper mapper = JsonUtil.jsonMapper();
    String json = mapper.writeValueAsString(source);
    NotificationSourceConfig restored = mapper.readValue(json, NotificationSourceConfig.class);

    assertEquals(source.getId(), restored.getId());
    assertEquals(source.getRssUrl(), restored.getRssUrl());
  }

  @Test
  public void testJsonRoundTrip_customPluginWithPluginId() throws Exception {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId("my-plugin");
    source.setName("My Plugin Notifications");
    source.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN);
    source.setEnabled(true);
    source.setPluginId("my-plugin");
    source.setPollIntervalMinutes("60");

    ObjectMapper mapper = JsonUtil.jsonMapper();
    String json = mapper.writeValueAsString(source);
    NotificationSourceConfig restored = mapper.readValue(json, NotificationSourceConfig.class);

    assertEquals("my-plugin", restored.getPluginId());
    assertEquals("my-plugin", restored.getId());
  }

  @Test
  public void testGetPluginId_fallbackToIdWhenPluginIdMissing() {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId("my-plugin");
    source.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN);
    // Do not set pluginId - simulate JSON load where plugin.id was missing
    source.getProperties().clear();

    assertEquals("my-plugin", source.getPluginId());
  }

  @Test
  public void testGetPluginId_returnsPluginIdWhenSet() {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId("source-abc123");
    source.setPluginId("my-plugin");

    assertEquals("my-plugin", source.getPluginId());
  }

  @Test
  public void testGetPluginId_returnsNullWhenBothMissing() {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN);
    source.getProperties().clear();
    source.setId(null);

    assertNull(source.getPluginId());
  }

  @Test
  public void testJsonRoundTrip_listOfSources() throws Exception {
    NotificationSourceConfig github = new NotificationSourceConfig();
    github.setId("github-apache-hop");
    github.setName("Apache Hop");
    github.setType(NotificationSourceConfig.SourceType.GITHUB_RELEASES);
    github.setGithubOwner("apache");
    github.setGithubRepo("hop");

    NotificationSourceConfig rss = new NotificationSourceConfig();
    rss.setId("rss-1");
    rss.setName("RSS Feed");
    rss.setType(NotificationSourceConfig.SourceType.RSS_FEED);
    rss.setRssUrl("https://example.com/feed.xml");

    List<NotificationSourceConfig> sources = List.of(github, rss);
    ObjectMapper mapper = JsonUtil.jsonMapper();
    String json = mapper.writeValueAsString(sources);
    List<NotificationSourceConfig> restored =
        mapper.readValue(json, new TypeReference<List<NotificationSourceConfig>>() {});

    assertNotNull(restored);
    assertEquals(2, restored.size());
    assertEquals("github-apache-hop", restored.get(0).getId());
    assertEquals("rss-1", restored.get(1).getId());
  }
}
