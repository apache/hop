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

package org.apache.hop.core.xml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.Const;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class XmlParserFactoryProducerTest {

  private String previousValue;

  @BeforeEach
  void saveVariable() {
    previousValue = System.getProperty(Const.XML_ALLOW_DOCTYPE_DECL);
  }

  @AfterEach
  void restoreVariable() {
    if (previousValue == null) {
      System.clearProperty(Const.XML_ALLOW_DOCTYPE_DECL);
    } else {
      System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, previousValue);
    }
  }

  /** Only N refuses a DOCTYPE, as the secure DocumentBuilderFactory always did. */
  @ParameterizedTest
  @CsvSource(
      value = {"N, false", "Y, true", "y, true", "true, true", "'', true", "<unset>, true"},
      nullValues = "<unset>")
  void doctypeIsAllowedUnlessTheVariableIsN(String value, boolean allowed) {
    if (value == null) {
      System.clearProperty(Const.XML_ALLOW_DOCTYPE_DECL);
    } else {
      System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, value);
    }
    assertEquals(allowed, XmlParserFactoryProducer.isDoctypeAllowed());
  }
}
