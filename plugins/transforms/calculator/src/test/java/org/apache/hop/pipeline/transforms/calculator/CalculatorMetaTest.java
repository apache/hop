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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.lineage.api.IDeclaresTransformLogic.FieldLogic;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class CalculatorMetaTest {

  @Test
  void testXmlRoundTrip() throws Exception {
    CalculatorMeta meta = new CalculatorMeta();
    meta.setFailIfNoFile(true);
    for (int i = 0; i < 100; i++) {
      meta.getFunctions().add(generateTestFunction());
    }

    String xml = meta.getXml();
    assertNotNull(xml);

    // Re-load it into a new meta
    //
    CalculatorMeta meta2 = new CalculatorMeta();
    String transformXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + xml
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    meta2.loadXml(XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG), null);

    // Verify the functions...
    assertEquals(meta.isFailIfNoFile(), meta2.isFailIfNoFile());
    assertEquals(meta.getFunctions().size(), meta2.getFunctions().size());
    for (int i = 0; i < meta.getFunctions().size(); i++) {
      CalculatorMetaFunction function = meta.getFunctions().get(i);
      CalculatorMetaFunction function2 = meta2.getFunctions().get(i);
      assertEquals(function, function2);
    }
  }

  private static final Random rand = new Random();

  private static CalculatorMetaFunction generateTestFunction() {
    CalculationType[] types = CalculationType.values();

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

  @Test
  void eachCalculationIsItsCodeAndItsFields() {
    CalculatorMeta meta = new CalculatorMeta();
    meta.getFunctions().add(function("tax", CalculationType.MULTIPLY, "total", "rate", null));
    meta.getFunctions().add(function("one", CalculationType.CONSTANT, "1", "", ""));
    meta.getFunctions().add(function("skipped", CalculationType.NONE, "a", null, null));
    meta.getFunctions().add(function("", CalculationType.CONSTANT, "2", null, null));

    assertEquals(
        List.of(
            new FieldLogic("tax", "calculator", "MULTIPLY(total, rate)"),
            new FieldLogic("one", "calculator", "CONSTANT(1)")),
        meta.transformLogic(new Variables()));
  }

  private static CalculatorMetaFunction function(
      String field, CalculationType type, String a, String b, String c) {
    CalculatorMetaFunction function = new CalculatorMetaFunction();
    function.setFieldName(field);
    function.setCalcType(type);
    function.setFieldA(a);
    function.setFieldB(b);
    function.setFieldC(c);
    return function;
  }
}
