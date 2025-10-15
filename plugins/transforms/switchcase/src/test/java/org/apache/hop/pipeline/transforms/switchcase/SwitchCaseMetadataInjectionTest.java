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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SwitchCaseMetadataInjectionTest extends BaseMetadataInjectionTestJunit5<SwitchCaseMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    super.setup(new SwitchCaseMeta());
  }

  @Test
  void test() throws Exception {
    check("FIELD_NAME", () -> meta.getFieldName());
    check("VALUE_TYPE", () -> meta.getCaseValueType());
    check("VALUE_DECIMAL", () -> meta.getCaseValueDecimal());
    check("VALUE_GROUP", () -> meta.getCaseValueGroup());
    check("VALUE_FORMAT", () -> meta.getCaseValueFormat());
    check("CONTAINS", () -> meta.isUsingContains());
    check("DEFAULT_TARGET_TRANSFORM_NAME", () -> meta.getDefaultTargetTransformName());
    check("SWITCH_CASE_TARGET.CASE_VALUE", () -> meta.getCaseTargets().get(0).getCaseValue());
    check(
        "SWITCH_CASE_TARGET.CASE_TARGET_TRANSFORM_NAME",
        () -> meta.getCaseTargets().get(0).getCaseTargetTransformName());
  }
}
