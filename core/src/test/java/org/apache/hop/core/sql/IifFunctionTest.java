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

package org.apache.hop.core.sql;

import org.apache.hop.core.Condition;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IifFunctionTest {

  @Before
  public void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testIifFunction01() throws Exception {

    IRowMeta serviceFields = generateTestRowMeta();

    String conditionClause = "B>5000";
    String trueValueString = "'Big'";
    String falseValueString = "'Small'";

    IifFunction function =
        new IifFunction(
            "Service", conditionClause, trueValueString, falseValueString, serviceFields);
    assertNotNull(function.getSqlCondition());
    Condition condition = function.getSqlCondition().getCondition();
    assertNotNull(condition);
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">", condition.getFunctionDesc());
    assertEquals("5000", condition.getRightExactString());

    // test the value data type determination
    //
    assertNotNull(function.getTrueValue());
    assertEquals(IValueMeta.TYPE_STRING, function.getTrueValue().getValueMeta().getType());
    assertEquals("Big", function.getTrueValue().getValueData());
    assertNotNull(function.getFalseValue());
    assertEquals(IValueMeta.TYPE_STRING, function.getFalseValue().getValueMeta().getType());
    assertEquals("Small", function.getFalseValue().getValueData());
  }

  private IRowMeta generateTestRowMeta() {
    IRowMeta rowMeta = new RowMetaBuilder().addString("A", 50).addInteger("B", 7).build();
    return rowMeta;
  }
}
