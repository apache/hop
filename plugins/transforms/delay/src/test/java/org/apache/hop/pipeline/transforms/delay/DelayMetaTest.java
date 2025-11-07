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

package org.apache.hop.pipeline.transforms.delay;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class DelayMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testTransformMeta() throws HopException {
    List<String> attributes =
        Arrays.asList(
            "timeout", "timeoutField", "scaletime", "scaleTimeFromField", "scaleTimeField");

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("timeout", "getTimeout");
    getterMap.put("timeoutField", "getTimeoutField");
    getterMap.put("scaletime", "getScaletime");
    getterMap.put("scaleTimeFromField", "isScaleTimeFromField");
    getterMap.put("scaleTimeField", "getScaleTimeField");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("timeout", "setTimeout");
    setterMap.put("timeoutField", "setTimeoutField");
    setterMap.put("scaletime", "setScaletime");
    setterMap.put("scaleTimeFromField", "setScaleTimeFromField");
    setterMap.put("scaleTimeField", "setScaleTimeField");

    LoadSaveTester<DelayMeta> loadSaveTester =
        new LoadSaveTester<>(DelayMeta.class, attributes, getterMap, setterMap);
    loadSaveTester.testSerialization();
  }
}
