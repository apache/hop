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
package org.apache.hop.pipeline.transforms.joinrows;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class JoinRowsMetaInjectionTest extends BaseMetadataInjectionTest<JoinRowsMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup(new JoinRowsMeta());
  }

  @Test
  public void test() throws Exception {
    check("TEMP_DIR", () -> meta.getDirectory());
    check("TEMP_FILE_PREFIX", () -> meta.getPrefix());
    check("MAX_CACHE_SIZE", () -> meta.getCacheSize());
    check("MAIN_TRANSFORM", () -> meta.getMainTransformName());
    skipPropertyTest("CONDITION");
    skipPropertyTest("negated");
    skipPropertyTest("isnull");
    skipPropertyTest("leftvalue");
    skipPropertyTest("precision");
    skipPropertyTest("length");
    skipPropertyTest("type");
    skipPropertyTest("operator");
    skipPropertyTest("rightvalue");
    skipPropertyTest("function");
    skipPropertyTest("name");
    skipPropertyTest("text");
    skipPropertyTest("mask");
  }
}
