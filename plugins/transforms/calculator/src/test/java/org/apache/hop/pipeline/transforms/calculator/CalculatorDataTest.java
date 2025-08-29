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

package org.apache.hop.pipeline.transforms.calculator;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class CalculatorDataTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void dataReturnsCachedValues() throws Exception {
    HopEnvironment.init();

    CalculatorData data = new CalculatorData();
    IValueMeta valueMeta = data.getValueMetaFor(IValueMeta.TYPE_INTEGER, null);
    IValueMeta shouldBeTheSame = data.getValueMetaFor(IValueMeta.TYPE_INTEGER, null);
    assertSame(
        valueMeta, shouldBeTheSame, "CalculatorData should cache loaded value meta instances");
  }
}
