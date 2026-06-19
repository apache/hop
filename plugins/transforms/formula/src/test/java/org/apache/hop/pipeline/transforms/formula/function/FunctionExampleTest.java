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

package org.apache.hop.pipeline.transforms.formula.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/** Unit test for {@link FunctionExample} */
class FunctionExampleTest {

  private static final String EXAMPLE_XML =
      """
					<example>
					  <expression>ABS(2)</expression>
					  <result>2</result>
					  <level>1</level>
					  <comment>Positive values return unchanged.</comment>
					</example>
					""";

  @Test
  void constructorSetsAllFields() {
    FunctionExample example =
        new FunctionExample("ABS(2)", "2", "1", "Positive values return unchanged.");

    assertEquals("ABS(2)", example.getExpression());
    assertEquals("2", example.getResult());
    assertEquals("1", example.getLevel());
    assertEquals("Positive values return unchanged.", example.getComment());
  }

  @Test
  void parsesFromXmlNode() throws HopXmlException {
    Node node = XmlHandler.loadXmlString(EXAMPLE_XML, FunctionExample.XML_TAG);
    FunctionExample example = new FunctionExample(node);

    assertEquals("ABS(2)", example.getExpression());
    assertEquals("2", example.getResult());
    assertEquals("1", example.getLevel());
    assertEquals("Positive values return unchanged.", example.getComment());
  }

  @Test
  void parsesFromXmlNodeWithMissingOptionalTags() throws HopXmlException {
    String xml =
        """
						<example>
						  <expression>1+1</expression>
						  <result>2</result>
						</example>
						""";
    FunctionExample example = new FunctionExample(XmlHandler.loadXmlString(xml, "example"));

    assertEquals("1+1", example.getExpression());
    assertEquals("2", example.getResult());
    assertNull(example.getLevel());
    assertNull(example.getComment());
  }
}
