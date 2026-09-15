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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;

/** What the source dialog's form carries, and what it writes back. */
public class NotificationSourceModelTest {

  @Test
  public void testAGithubSourceSurvivesTheRoundTrip() {
    NotificationSourceConfig config = new NotificationSourceConfig();
    config.setId("github-apache-hop");
    config.setName("Apache Hop Releases");
    config.setType(NotificationSourceConfig.SourceType.GITHUB_RELEASES);
    config.setEnabled(true);
    config.setGithubOwner("apache");
    config.setGithubRepo("hop");
    config.setGithubIncludePrereleases(true);
    config.setMinimumVersion("2.19.0");
    config.setPollIntervalMinutes("30");
    config.setDaysToGoBack("7");
    config.setColor("#FF5733");

    NotificationSourceConfig written = new NotificationSourceConfig();
    NotificationSourceModel.fromConfig(config).toConfig(written);

    assertEquals("Apache Hop Releases", written.getName());
    assertEquals(NotificationSourceConfig.SourceType.GITHUB_RELEASES, written.getType());
    assertTrue(written.isEnabled());
    assertEquals("apache", written.getGithubOwner());
    assertEquals("hop", written.getGithubRepo());
    assertTrue(written.isGithubIncludePrereleases());
    assertEquals("2.19.0", written.getMinimumVersion());
    assertEquals("30", written.getPollIntervalMinutes());
    assertEquals("7", written.getDaysToGoBack());
    assertEquals("#FF5733", written.getColor());
  }

  @Test
  public void testTheGithubUrlIsOfferedForAConfiguredRepository() {
    NotificationSourceConfig config = new NotificationSourceConfig();
    config.setType(NotificationSourceConfig.SourceType.GITHUB_RELEASES);
    config.setGithubOwner("apache");
    config.setGithubRepo("hop");

    assertEquals(
        "https://github.com/apache/hop", NotificationSourceModel.fromConfig(config).getGithubUrl());
  }

  @Test
  public void testOnlyTheChosenTypesFieldsAreWritten() {
    // Switching a source to RSS and back must not leave it carrying a repository it never polls.
    NotificationSourceModel model = new NotificationSourceModel();
    model.setName("A feed");
    model.setType(NotificationSourceConfig.SourceType.RSS_FEED.getDisplayName());
    model.setRssUrl("https://hop.apache.org/blog/feed.xml");
    model.setGithubOwner("apache");
    model.setGithubRepo("hop");

    NotificationSourceConfig written = new NotificationSourceConfig();
    model.toConfig(written);

    assertEquals("https://hop.apache.org/blog/feed.xml", written.getRssUrl());
    assertNull(written.getGithubOwner());
    assertNull(written.getGithubRepo());
  }

  @Test
  public void testAPluginSourceIsIdentifiedByItsPluginId() {
    // The provider is registered under its plugin id, so the source has to carry the same one.
    NotificationSourceModel model = new NotificationSourceModel();
    model.setName("Marketplace");
    model.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN.getDisplayName());
    model.setPluginId("marketplace-plugin-updates");

    NotificationSourceConfig written = new NotificationSourceConfig();
    model.toConfig(written);

    assertEquals("marketplace-plugin-updates", written.getId());
    assertEquals("marketplace-plugin-updates", written.getPluginId());
  }

  @Test
  public void testAPluginSourceIsNotAskedForCredentials() {
    // A plugin's provider authenticates itself.
    NotificationSourceModel model = new NotificationSourceModel();
    model.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN.getDisplayName());

    Set<String> hidden = model.widgetsToHide();

    assertTrue(hidden.contains(NotificationSourceModel.WIDGET_USERNAME));
    assertTrue(hidden.contains(NotificationSourceModel.WIDGET_PASSWORD));
    assertFalse(hidden.contains(NotificationSourceModel.WIDGET_PLUGIN_ID));
  }

  @Test
  public void testAGithubSourceShowsOnlyGithubFields() {
    NotificationSourceModel model = new NotificationSourceModel();
    model.setType(NotificationSourceConfig.SourceType.GITHUB_RELEASES.getDisplayName());

    Set<String> hidden = model.widgetsToHide();

    assertFalse(hidden.contains(NotificationSourceModel.WIDGET_GITHUB_OWNER));
    assertFalse(hidden.contains(NotificationSourceModel.WIDGET_MINIMUM_VERSION));
    assertTrue(hidden.contains(NotificationSourceModel.WIDGET_RSS_URL));
    assertTrue(hidden.contains(NotificationSourceModel.WIDGET_PLUGIN_ID));
  }

  @Test
  public void testAnRssSourceShowsOnlyItsUrl() {
    NotificationSourceModel model = new NotificationSourceModel();
    model.setType(NotificationSourceConfig.SourceType.RSS_FEED.getDisplayName());

    Set<String> hidden = model.widgetsToHide();

    assertFalse(hidden.contains(NotificationSourceModel.WIDGET_RSS_URL));
    assertTrue(hidden.contains(NotificationSourceModel.WIDGET_GITHUB_URL));
    assertTrue(hidden.contains(NotificationSourceModel.WIDGET_PARSE_GITHUB_URL));
    assertTrue(hidden.contains(NotificationSourceModel.WIDGET_PLUGIN_ID));
  }

  @Test
  public void testAnUnknownTypeFallsBackToTheFirstOne() {
    NotificationSourceModel model = new NotificationSourceModel();
    model.setType("something a future version writes");

    assertEquals(NotificationSourceConfig.SourceType.values()[0], model.selectedType());
  }

  @Test
  public void testTheParseButtonFillsInTheOwnerAndRepository() {
    NotificationSourceModel model = new NotificationSourceModel();
    model.setGithubUrl("https://github.com/apache/hop");

    model.parseGithubUrl(model);

    assertEquals("apache", model.getGithubOwner());
    assertEquals("hop", model.getGithubRepo());
  }

  @Test
  public void testTheParseButtonLeavesAHalfTypedUrlAlone() {
    NotificationSourceModel model = new NotificationSourceModel();
    model.setGithubOwner("apache");
    model.setGithubRepo("hop");
    model.setGithubUrl("https://git");

    model.parseGithubUrl(model);

    assertEquals("apache", model.getGithubOwner());
    assertEquals("hop", model.getGithubRepo());
  }

  @Test
  public void testTheTypeComboOffersEveryType() {
    assertEquals(
        NotificationSourceConfig.SourceType.values().length,
        new NotificationSourceModel().getTypeNames(null, null).size());
  }

  @Test
  public void testTheParseButtonKeepsAGoodRepositoryWhenTheUrlIsWrong() {
    // A typo in the URL must not throw away an owner and repository that were already right.
    NotificationSourceModel model = new NotificationSourceModel();
    model.setGithubOwner("apache");
    model.setGithubRepo("hop");
    model.setGithubUrl("not a repository at all");

    // No display in a headless test, so the error dialog is skipped; the fields are the assertion.
    model.parseGithubUrl(model);

    assertEquals("apache", model.getGithubOwner());
    assertEquals("hop", model.getGithubRepo());
  }
}
