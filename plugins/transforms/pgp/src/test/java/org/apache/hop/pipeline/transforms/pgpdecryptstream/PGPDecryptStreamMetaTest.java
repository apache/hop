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
package org.apache.hop.pipeline.transforms.pgpdecryptstream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PGPDecryptStreamMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  LoadSaveTester loadSaveTester;
  Class<PGPDecryptStreamMeta> testMetaClass = PGPDecryptStreamMeta.class;

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes =
        Arrays.asList(
            "gPGLocation",
            "passphrase",
            "passphraseFieldName",
            "passphraseFieldName",
            "streamField",
            "resultFieldName");

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap);
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }
}
