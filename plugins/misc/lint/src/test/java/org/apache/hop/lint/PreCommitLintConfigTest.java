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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class PreCommitLintConfigTest {

  @Test
  public void preCommitDisabledByDefault() {
    LinterConfigPlugin config = new LinterConfigPlugin();
    assertFalse(config.isPreCommitLintEnabled());
  }

  @Test
  public void shouldBlockErrorsAlwaysWhenWarningsEnabled() {
    LinterConfigPlugin config = new LinterConfigPlugin();
    config.setPreCommitBlockWarnings(true);
    assertTrue(config.shouldBlockCommitForSeverity("ERROR"));
    assertTrue(config.shouldBlockCommitForSeverity("WARNING"));
    assertFalse(config.shouldBlockCommitForSeverity("INFO"));
  }

  @Test
  public void shouldBlockErrorsOnlyByDefault() {
    LinterConfigPlugin config = new LinterConfigPlugin();
    assertTrue(config.shouldBlockCommitForSeverity("ERROR"));
    assertFalse(config.shouldBlockCommitForSeverity("WARNING"));
  }

  @Test
  public void isLintableCommitFileIncludesPipelinesAndWorkflows() {
    LinterConfigPlugin config = new LinterConfigPlugin();
    assertTrue(PreCommitLintExtension.isLintableCommitFile("/project/pipeline.hpl", config));
    assertTrue(PreCommitLintExtension.isLintableCommitFile("/project/workflow.hwf", config));
  }

  @Test
  public void isLintableCommitFileIncludesMetadataWhenEnabled() {
    LinterConfigPlugin config = new LinterConfigPlugin();
    config.setPreCommitIncludeMetadata(true);
    assertTrue(
        PreCommitLintExtension.isLintableCommitFile("/project/metadata/rdbms/local.json", config));
  }

  @Test
  public void isLintableCommitFileExcludesMetadataWhenDisabled() {
    LinterConfigPlugin config = new LinterConfigPlugin();
    config.setPreCommitIncludeMetadata(false);
    assertFalse(
        PreCommitLintExtension.isLintableCommitFile("/project/metadata/rdbms/local.json", config));
  }
}
