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

package org.apache.hop.pipeline.transforms.edi2xml;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Assert;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Edi2XmlMetaTest {
  @Test
  public void testSerialization() throws Exception {
    Edi2XmlMeta meta = TransformSerializationTestUtil.testSerialization(
            "/edi-to-xml-transform.xml", Edi2XmlMeta.class);

    Assert.assertNotNull(meta.getInputField());
    Assert.assertNotNull(meta.getOutputField());

    org.junit.Assert.assertEquals(meta.getInputField(), "edi");
    org.junit.Assert.assertEquals(meta.getOutputField(), "xml");
  }
}
