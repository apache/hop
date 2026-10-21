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

package org.apache.hop.pipeline.transforms.valuemapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * A new String target field whose mapped targets and non-match default are all blank has an
 * undefined length (-1). Up to 2.17 a saved {@code <target_value/>} or {@code <non_match_default/>}
 * loaded as null; since 2.18 it loads as an empty string, which must not size the field to 0.
 */
class ValueMapperBlankTargetLengthTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void blankTargetsAndDefaultLeaveLengthUndefined() throws Exception {
    ValueMapperMeta meta = loadMeta("<non_match_default/>", "<target_value/>", "<target_value/>");

    assertEquals(-1, targetLength(meta));
  }

  @Test
  void nonBlankTargetsSizeFieldToLongestValue() throws Exception {
    ValueMapperMeta meta =
        loadMeta(
            "<non_match_default>n/a</non_match_default>",
            "<target_value>Belgium</target_value>",
            "<target_value>Netherlands</target_value>");

    assertEquals("Netherlands".length(), targetLength(meta));
  }

  private static int targetLength(ValueMapperMeta meta) {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("code"));
    meta.getFields(row, "mapper", null, new TransformMeta(), new Variables(), null);
    IValueMeta target = row.searchValueMeta("country");
    assertNotNull(target, "Target field was not added");
    return target.getLength();
  }

  private static ValueMapperMeta loadMeta(String defaultTag, String target1, String target2)
      throws Exception {
    String xml =
        "<transform>"
            + "<field_to_use>code</field_to_use>"
            + "<target_field>country</target_field>"
            + defaultTag
            + "<target_type>String</target_type>"
            + "<fields>"
            + "<field><source_value>BE</source_value>"
            + target1
            + "</field>"
            + "<field><source_value>NL</source_value>"
            + target2
            + "</field>"
            + "</fields>"
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
