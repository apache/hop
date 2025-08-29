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

package org.apache.hop.pipeline.transforms.xml.addxml;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class AddXmlMetaInjectionTest extends BaseMetadataInjectionTestJunit5<AddXmlMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() {
    try {
      setup(new AddXmlMeta());
    } catch (Exception e) {

    }
  }

  @Test
  void test() throws Exception {

    check("OMIT_XML_HEADER", () -> meta.isOmitXMLheader());

    check("OMIT_NULL_VALUES", () -> meta.isOmitNullValues());

    check("ENCODING", () -> meta.getEncoding());

    check("VALUE_NAME", () -> meta.getValueName());

    check("ROOT_NODE", () -> meta.getRootNode());

    check("OUTPUT_FIELD_NAME", () -> meta.getOutputFields()[0].getFieldName());

    check("OUTPUT_ELEMENT_NAME", () -> meta.getOutputFields()[0].getElementName());

    String[] typeNames = ValueMetaBase.getAllTypes();
    checkStringToInt(
        "OUTPUT_TYPE",
        () -> meta.getOutputFields()[0].getType(),
        typeNames,
        getTypeCodes(typeNames));

    check("OUTPUT_FORMAT", () -> meta.getOutputFields()[0].getFormat());

    check("OUTPUT_LENGTH", () -> meta.getOutputFields()[0].getLength());

    check("OUTPUT_PRECISION", () -> meta.getOutputFields()[0].getPrecision());

    check("OUTPUT_CURRENCY_SYMBOL", () -> meta.getOutputFields()[0].getCurrencySymbol());

    check("OUTPUT_DECIMAL_SYMBOL", () -> meta.getOutputFields()[0].getDecimalSymbol());

    check("OUTPUT_GROUPING_SYMBOL", () -> meta.getOutputFields()[0].getGroupingSymbol());

    check("OUTPUT_ATTRIBUTE", () -> meta.getOutputFields()[0].isAttribute());

    check("OUTPUT_ATTRIBUTE_PARENT_NAME", () -> meta.getOutputFields()[0].getAttributeParentName());

    check("OUTPUT_NULL_STRING", () -> meta.getOutputFields()[0].getNullString());
  }
}
