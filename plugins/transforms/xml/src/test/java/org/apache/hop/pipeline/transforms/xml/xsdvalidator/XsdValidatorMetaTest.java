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

package org.apache.hop.pipeline.transforms.xml.xsdvalidator;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class XsdValidatorMetaTest {
  @Test
  void testSerializationRoundTrip() throws Exception {
    XsdValidatorMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/xsd-validator.xml", XsdValidatorMeta.class);

    Assertions.assertEquals("xsd-filename", meta.getXsdFilename());
    Assertions.assertEquals("xml-field", meta.getXmlStream());
    Assertions.assertEquals("result", meta.getResultFieldName());
    Assertions.assertTrue(meta.isAddValidationMessage());
    Assertions.assertEquals("ValidationMsgField", meta.getValidationMessageField());
    Assertions.assertEquals("invalid-field", meta.getIfXmlInvalid());
    Assertions.assertTrue(meta.isOutputStringField());
    Assertions.assertTrue(meta.isXmlSourceFile());
    Assertions.assertEquals("xsd-defined-field", meta.getXsdDefinedField());
    Assertions.assertEquals("filename", meta.getXsdSource());
    Assertions.assertTrue(meta.isAllowExternalEntities());
  }
}
