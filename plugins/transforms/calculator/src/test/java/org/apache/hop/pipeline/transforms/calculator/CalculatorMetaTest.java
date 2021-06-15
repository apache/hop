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

import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

public class CalculatorMetaTest {

  @Test
  public void testXmlRoundTrip() throws Exception {
    CalculatorMeta meta = new CalculatorMeta();
    meta.setFailIfNoFile(true);
    for (int i = 0; i < 100; i++) {
      meta.getFunctions().add(generateTestFunction());
    }

    String xml = meta.getXml();
    Assert.assertNotNull(xml);

    // Re-load it into a new meta
    //
    CalculatorMeta meta2 = new CalculatorMeta();
    String transformXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + xml
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    meta2.loadXml(XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG), null);

    // Verify the functions...
    Assert.assertEquals(meta.isFailIfNoFile(), meta2.isFailIfNoFile());
    Assert.assertEquals(meta.getFunctions().size(), meta2.getFunctions().size());
    for (int i = 0; i < meta.getFunctions().size(); i++) {
      CalculatorMetaFunction function = meta.getFunctions().get(i);
      CalculatorMetaFunction function2 = meta2.getFunctions().get(i);
      Assert.assertEquals(function, function2);
    }
  }

  private static final Random rand = new Random();

  private static CalculatorMetaFunction generateTestFunction() {
    CalculatorMetaFunction.CalculationType[] types =
        CalculatorMetaFunction.CalculationType.values();
    String[] valueTypes = {"String", "Number", "Date", "Integer", "Boolean"};

    CalculatorMetaFunction rtn = new CalculatorMetaFunction();
    rtn.setCalcType(types[Math.abs(rand.nextInt(types.length))]);
    rtn.setConversionMask(UUID.randomUUID().toString());
    rtn.setCurrencySymbol(UUID.randomUUID().toString());
    rtn.setDecimalSymbol(UUID.randomUUID().toString());
    rtn.setFieldA(UUID.randomUUID().toString());
    rtn.setFieldB(UUID.randomUUID().toString());
    rtn.setFieldC(UUID.randomUUID().toString());
    rtn.setFieldName(UUID.randomUUID().toString());
    rtn.setGroupingSymbol(UUID.randomUUID().toString());
    rtn.setValueLength(rand.nextInt(50));
    rtn.setValuePrecision(rand.nextInt(9));
    rtn.setValueType(valueTypes[Math.abs(rand.nextInt(valueTypes.length))]);
    rtn.setRemovedFromResult(rand.nextBoolean());
    return rtn;
  }
}
