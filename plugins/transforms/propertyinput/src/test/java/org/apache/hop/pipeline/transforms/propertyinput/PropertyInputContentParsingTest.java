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
package org.apache.hop.pipeline.transforms.propertyinput;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.propertyinput.PropertyInputMeta.KeyValue;
import org.apache.hop.pipeline.transforms.propertyinput.PropertyInputMeta.PIField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PropertyInputContentParsingTest extends BasePropertyParsingTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testDefaultOptions() throws Exception {
    init("default.properties");

    PIField f1 = new PIField("f1");
    f1.setColumn(KeyValue.KEY);
    PIField f2 = new PIField("f2");
    f2.setColumn(KeyValue.VALUE);
    setFields(f1, f2);

    process();

    check(new Object[][] {{"f1", "d1"}, {"f2", "d2"}});
  }
}
