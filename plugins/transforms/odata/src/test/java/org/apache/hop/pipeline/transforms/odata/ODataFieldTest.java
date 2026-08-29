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

package org.apache.hop.pipeline.transforms.odata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link ODataField} */
class ODataFieldTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void defaultConstructorUsesStringType() {
    ODataField field = new ODataField();
    assertEquals("", field.getName());
    assertEquals("", field.getPath());
    assertEquals(IValueMeta.TYPE_STRING, field.getType());
    assertEquals("", field.getFormat());
  }

  @Test
  void copyConstructorAndCloneAreIndependentCopies() {
    ODataField original = new ODataField("Id", "Id", IValueMeta.TYPE_INTEGER, "#");
    ODataField copy = new ODataField(original);
    ODataField cloned = original.clone();

    assertEquals(original, copy);
    assertEquals(original, cloned);
    assertNotSame(original, copy);
    assertNotSame(original, cloned);

    copy.setName("Other");
    assertEquals("Id", original.getName());
  }

  @Test
  void equalsAndHashCodeUseAllProperties() {
    ODataField a = new ODataField("Id", "Id", IValueMeta.TYPE_INTEGER, "#");
    ODataField b = new ODataField("Id", "Id", IValueMeta.TYPE_INTEGER, "#");
    ODataField c = new ODataField("Name", "Id", IValueMeta.TYPE_INTEGER, "#");

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
    assertNotEquals(a, null);
    assertNotEquals(a, "Id");
  }

  @Test
  void toValueMetaUsesTypeNameOriginAndFormat() throws Exception {
    ODataField field = new ODataField("Amount", "Price", IValueMeta.TYPE_NUMBER, "0.00");
    IValueMeta valueMeta = field.toValueMeta("OData Input", null);

    assertEquals("Amount", valueMeta.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, valueMeta.getType());
    assertEquals("OData Input", valueMeta.getOrigin());
    assertEquals("0.00", valueMeta.getConversionMask());
  }

  @Test
  void toValueMetaTreatsNoneAsStringAndResolvesVariables() throws Exception {
    ODataField field = new ODataField("${FIELD_NAME}", "Name", IValueMeta.TYPE_NONE, "");
    Variables variables = new Variables();
    variables.setVariable("FIELD_NAME", "ProductName");

    IValueMeta valueMeta = field.toValueMeta("odata", variables);

    assertEquals("ProductName", valueMeta.getName());
    assertEquals(IValueMeta.TYPE_STRING, valueMeta.getType());
  }
}
