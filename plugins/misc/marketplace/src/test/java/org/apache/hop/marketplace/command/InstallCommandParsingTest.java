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

import java.util.List;
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
  void atLeastOneCoordinateIsRequired() {
    assertThrows(CommandLine.MissingParameterException.class, this::parseNothing);
  }

  private void parseNothing() {
    new CommandLine(new MarketplaceCommand.InstallCommand()).parseArgs();
  }
}
