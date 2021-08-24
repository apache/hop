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

package org.apache.hop.pipeline.transforms.calculator;

import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.Test;

import static org.junit.Assert.*;

public class CalculatorMetaFunctionTest {

  @Test
  public void testEquals() {
    CalculatorMetaFunction meta1 = new CalculatorMetaFunction();
    CalculatorMetaFunction meta2 = (CalculatorMetaFunction) meta1.clone();
    assertNotSame(meta1, meta2);

    assertFalse(meta1.equals(null));
    assertFalse(meta1.equals(new Object()));
    assertTrue(meta1.equals(meta2));

    meta2.setCalcType(CalculatorMetaFunction.CalculationType.ADD_DAYS);
    assertFalse(meta1.equals(meta2));
  }

  @Test
  public void testXmlSerialization() throws Exception {
    CalculatorMetaFunction function =
        new CalculatorMetaFunction(
            "copyA",
            CalculatorMetaFunction.CalculationType.COPY_OF_FIELD,
            "A",
            null,
            null,
            "String",
            100,
            -1,
            null,
            null,
            null,
            null,
            false);
    String xml = XmlMetadataUtil.serializeObjectToXml(function);
    assertEquals(
        "<calc_type>COPY_FIELD</calc_type>"
            + System.lineSeparator()
            + "<field_a>A</field_a>"
            + System.lineSeparator()
            + "<field_name>copyA</field_name>"
            + System.lineSeparator()
            + "<remove>N</remove>"
            + System.lineSeparator()
            + "<value_length>100</value_length>"
            + System.lineSeparator()
            + "<value_precision>-1</value_precision>"
            + System.lineSeparator()
            + "<value_type>String</value_type>"
            + System.lineSeparator(),
        xml);
  }
}
