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

package org.apache.hop.openformula.util;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hop.openformula.parser.ParseException;
import org.junit.jupiter.api.Test;

public class FormulaUtilTest {
  @Test
  public void testFormulaContextExtraction() {
    Object[][] test = {
      new Object[] {"test:IF()", true, "test", "IF()"},
      new Object[] {"=IF()", true, "report", "IF()"},
      new Object[] {"report-IF()", false, null, null},
      new Object[] {"=IF(:)", true, "report", "IF(:)"},
      new Object[] {"test=:IF()", false, null, null},
      new Object[] {"test asd:IF()", false, null, null},
    };

    for (Object[] objects : test) {
      String v = (String) objects[0];
      String[] strings = FormulaUtil.extractFormulaContext(v);
      if (strings[0] == null) {
        assertFalse((Boolean) objects[1], "Failure in " + v);
      } else {
        assertTrue((Boolean) objects[1], "Failure in " + v);
        assertEquals(objects[2], strings[0], "Failure in " + v);
        assertEquals(objects[3], strings[1], "Failure in " + v);
      }
    }
  }

  @Test
  public void getReferencesTest() throws ParseException {
    String[] references =
        FormulaUtil.getReferences("=CSVTEXT([parmTerritory];FALSE();\",\";\"'\")");
    assertTrue(references.length > 0, "References list is not empty");
    assertEquals("parmTerritory", references[0], "Formula has one reference to paramTerritory");
  }
}
