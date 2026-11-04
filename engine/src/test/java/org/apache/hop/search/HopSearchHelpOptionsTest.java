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
package org.apache.hop.search;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

/** -h/--help and -v/--version must be recognised (#9268). */
class HopSearchHelpOptionsTest {

  @ParameterizedTest
  @ValueSource(strings = {"-h", "--help"})
  void helpOptionIsRecognised(String option) {
    CommandLine cmd = new CommandLine(new HopSearch());

    CommandLine.ParseResult parseResult = cmd.parseArgs(option);

    assertTrue(parseResult.isUsageHelpRequested());
    assertFalse(parseResult.isVersionHelpRequested());
  }

  @Test
  void helpWithoutSearchStringDoesNotFailOnRequiredParameter() {
    CommandLine cmd = new CommandLine(new HopSearch());

    CommandLine.ParseResult parseResult = cmd.parseArgs("--help");

    assertTrue(parseResult.isUsageHelpRequested());
  }

  @ParameterizedTest
  @ValueSource(strings = {"-v", "--version"})
  void versionOptionIsRecognised(String option) {
    CommandLine cmd = new CommandLine(new HopSearch());

    CommandLine.ParseResult parseResult = cmd.parseArgs(option);

    assertTrue(parseResult.isVersionHelpRequested());
    assertFalse(parseResult.isUsageHelpRequested());
  }
}
