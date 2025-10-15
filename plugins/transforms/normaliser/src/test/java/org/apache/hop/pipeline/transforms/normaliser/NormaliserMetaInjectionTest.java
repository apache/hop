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

package org.apache.hop.pipeline.transforms.normaliser;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class NormaliserMetaInjectionTest extends BaseMetadataInjectionTestJunit5<NormaliserMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    setup(new NormaliserMeta());
  }

  @Test
  void test() throws Exception {
    check("TYPEFIELD", () -> meta.getTypeField());
    check("NAME", () -> meta.getNormaliserFields().get(0).getName());
    check("VALUE", () -> meta.getNormaliserFields().get(0).getValue());
    check("NORMALISED", () -> meta.getNormaliserFields().get(0).getNorm());
  }
}
