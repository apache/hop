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

package org.apache.hop.marketplace.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.marketplace.catalog.PluginDiscovery;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * {@code hop marketplace install} takes a list of coordinates. Arity is easy to break by accident
 * (an {@code index = "0"} positional silently swallows only the first word), so the contract is
 * pinned here rather than found out on a batch install.
 */
class InstallCommandParsingTest {

  private static List<String> parseCoordinates(String... args) {
    CommandLine commandLine = new CommandLine(new MarketplaceCommand.InstallCommand());
    return commandLine.parseArgs(args).matchedPositional(0).getValue();
  }

  @Test
  void severalCoordinatesAreAllCaptured() {
    assertEquals(
        List.of("datavault", "hop-tech-parquet", "hop-datavault:0.4.0-SNAPSHOT"),
        parseCoordinates("datavault", "hop-tech-parquet", "hop-datavault:0.4.0-SNAPSHOT"));
  }

  @Test
  void aSingleCoordinateStillWorks() {
    assertEquals(List.of("datavault"), parseCoordinates("datavault"));
  }

  @Test
  void optionsDoNotEndUpInTheCoordinateList() {
    CommandLine commandLine = new CommandLine(new MarketplaceCommand.InstallCommand());
    CommandLine.ParseResult parsed =
        commandLine.parseArgs("--repo", "local-nexus", "datavault", "hop-tech-parquet");
    assertEquals(List.of("datavault", "hop-tech-parquet"), parsed.matchedPositional(0).getValue());
    assertEquals("local-nexus", parsed.matchedOptionValue("--repo", null));
  }

  @Test
  void repoUrlAndCredentialsOptionsAreParsed() {
    CommandLine commandLine = new CommandLine(new MarketplaceCommand.InstallCommand());
    CommandLine.ParseResult parsed =
        commandLine.parseArgs(
            "--repo-url",
            "https://repository.data-hopper.com/repository/hop-community-plugins/",
            "--username",
            "testuser",
            "--password",
            "testpass",
            "--auth-type",
            "basic",
            "org.hopper:hopper-edw:0.10.0");
    assertEquals(List.of("org.hopper:hopper-edw:0.10.0"), parsed.matchedPositional(0).getValue());
    assertEquals(
        "https://repository.data-hopper.com/repository/hop-community-plugins/",
        parsed.matchedOptionValue("--repo-url", null));
    assertEquals("testuser", parsed.matchedOptionValue("--username", null));
    assertEquals("testpass", parsed.matchedOptionValue("--password", null));
    assertEquals("basic", parsed.matchedOptionValue("--auth-type", null));
  }

  @Test
  void repoIdAndRepoTypeOptionsAreParsed() {
    CommandLine commandLine = new CommandLine(new MarketplaceCommand.InstallCommand());
    CommandLine.ParseResult parsed =
        commandLine.parseArgs(
            "--repo-url",
            "https://maven.example/releases/",
            "--repo-id",
            "corporate-repo",
            "--repo-type",
            "maven",
            "com.acme:acme-plugin:1.0.0");
    assertEquals("corporate-repo", parsed.matchedOptionValue("--repo-id", null));
    assertEquals("maven", parsed.matchedOptionValue("--repo-type", null));
  }

  @Test
  void shortNameOnUnbrowsableRepositoryAsksForFullCoordinates() {
    MarketplaceRepository repo =
        new MarketplaceRepository("corporate-repo", "https://maven.example/releases/");
    repo.setBrowse(false);
    PluginDiscovery.InstallTarget target =
        new PluginDiscovery.InstallTarget(
            new MavenCoordinates("org.apache.hop", "acme-plugin", "1.0.0"), null);
    HopException ex =
        assertThrows(
            HopException.class,
            () ->
                MarketplaceCommand.InstallCommand.rejectUnresolvedShortName(
                    "acme-plugin:1.0.0", target, repo));
    assertTrue(ex.getMessage().contains("cannot be browsed"));
    assertTrue(ex.getMessage().contains("groupId:artifactId:version"));
  }

  @Test
  void fullCoordinateAndDiscoveredNameAreAcceptedWhenRepositoryCannotBeBrowsed() throws Exception {
    MarketplaceRepository repo =
        new MarketplaceRepository("corporate-repo", "https://maven.example/releases/");
    repo.setBrowse(false);
    MarketplaceCommand.InstallCommand.rejectUnresolvedShortName(
        "com.acme:acme-plugin:1.0.0",
        new PluginDiscovery.InstallTarget(
            new MavenCoordinates("com.acme", "acme-plugin", "1.0.0"), null),
        repo);
    MarketplaceCommand.InstallCommand.rejectUnresolvedShortName(
        "hop-tech-parquet",
        new PluginDiscovery.InstallTarget(
            new MavenCoordinates("org.apache.hop", "hop-tech-parquet", "2.20.0"), null, true),
        repo);
  }

  @Test
  void atLeastOneCoordinateIsRequired() {
    assertThrows(CommandLine.MissingParameterException.class, this::parseNothing);
  }

  private void parseNothing() {
    new CommandLine(new MarketplaceCommand.InstallCommand()).parseArgs();
  }
}
