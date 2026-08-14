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
package org.apache.hop.pipeline.transforms.multimapping;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MultiMappingMetaInjectionTest extends BaseMetadataInjectionTestJunit5<MultiMappingMeta> {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    MultiMappingMeta meta = new MultiMappingMeta();
    meta.getInputMappings().add(new MultiMappingInputDefinition());
    meta.getInputMappings().get(0).getValueRenames().add(new MultiMappingInputRename());
    meta.getOutputMappings().add(new MultiMappingOutputDefinition());
    meta.getOutputMappings().get(0).getValueRenames().add(new MultiMappingOutputRename());
    setup(meta);
  }

  @Test
  void test() throws Exception {
    check("filename", () -> meta.getFilename());
    check("RUN_CONFIGURATION", () -> meta.getRunConfigurationName());
    check("INPUT_SOURCE_TRANSFORM", () -> meta.getInputMappings().get(0).getInputTransformName());
    check("INPUT_MAPPING_TRANSFORM", () -> meta.getInputMappings().get(0).getOutputTransformName());
    check("INPUT_DESCRIPTION", () -> meta.getInputMappings().get(0).getDescription());
    check("INPUT_MAIN_PATH", () -> meta.getInputMappings().get(0).isMainDataPath());
    check("INPUT_RENAME_ON_OUTPUT", () -> meta.getInputMappings().get(0).isRenamingOnOutput());
    check(
        "OUTPUT_MAPPING_TRANSFORM", () -> meta.getOutputMappings().get(0).getInputTransformName());
    check(
        "OUTPUT_TARGET_TRANSFORM", () -> meta.getOutputMappings().get(0).getOutputTransformName());
    check("OUTPUT_DESCRIPTION", () -> meta.getOutputMappings().get(0).getDescription());
    check("OUTPUT_MAIN_PATH", () -> meta.getOutputMappings().get(0).isMainDataPath());
    check("OUTPUT_RENAME_ON_OUTPUT", () -> meta.getOutputMappings().get(0).isRenamingOnOutput());
    check("inherit_all_vars", () -> meta.getMappingParameters().isInheritingAllVariables());
    skipPropertyTest("variable");
    skipPropertyTest("input");
    skipPropertyTest("INPUT_RENAME_SOURCE");
    skipPropertyTest("INPUT_RENAME_TARGET");
    skipPropertyTest("OUTPUT_RENAME_SOURCE");
    skipPropertyTest("OUTPUT_RENAME_TARGET");
    skipPropertyTest("mappings");
    skipPropertyTest("parameters");
    skipPropertyTest("mapping");
    skipPropertyTest("connector");
  }
}
