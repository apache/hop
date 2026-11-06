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

package org.apache.hop.pipeline.transforms.drools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.junit.jupiter.api.Test;

class RulesExecutorDataTest {

  private static final String PRICE_CLASS_DRL =
      """
      package test;
      import org.apache.hop.pipeline.transforms.drools.Rules.Column;

      rule "Premium product"
      when
          Column(name == "list_price", ((Number) payload).doubleValue() >= 100)
      then
          insert(new Column("price_class", "String", "premium"));
      end

      rule "Standard product"
      when
          Column(name == "list_price", ((Number) payload).doubleValue() < 100)
      then
          insert(new Column("price_class", "String", "standard"));
      end

      rule "Input columns in this session"
      when
          Number($count : intValue) from accumulate(Column(externalSource == true), count(1))
      then
          insert(new Column("input_columns", "Integer", $count));
      end
      """;

  private static final String CONSTANT_DRL =
      """
      package other;
      import org.apache.hop.pipeline.transforms.drools.Rules.Column;

      rule "Always"
      when
          Column(name == "list_price")
      then
          insert(new Column("price_class", "String", "other rules"));
      end
      """;

  private static RulesExecutorData newData(String drl) throws RuleValidationException {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaNumber("list_price"));

    RulesExecutorData data = new RulesExecutorData();
    data.setRuleString(drl);
    data.initializeRules();
    data.initializeColumns(rowMeta);
    return data;
  }

  private static Object run(RulesExecutorData data, double listPrice, String column) {
    data.loadRow(new Object[] {listPrice});
    data.execute();
    Rules.Column result = (Rules.Column) data.fetchResult(column);
    return result == null ? null : result.getPayload();
  }

  @Test
  void rulesAreAppliedToEveryRow() throws Exception {
    RulesExecutorData data = newData(PRICE_CLASS_DRL);

    assertEquals("premium", run(data, 150.0, "price_class"));
    assertEquals("standard", run(data, 50.0, "price_class"));
    assertEquals("premium", run(data, 100.0, "price_class"));
    assertNull(run(data, 100.0, "no_such_column"));
  }

  @Test
  void everyRowGetsAFreshSession() throws Exception {
    RulesExecutorData data = newData(PRICE_CLASS_DRL);

    // With one session over several rows, the input columns of earlier rows would be counted too.
    for (int row = 0; row < 5; row++) {
      assertEquals(1, ((Number) run(data, row * 50.0, "input_columns")).intValue());
    }
  }

  @Test
  void transformsKeepTheirOwnRules() throws Exception {
    RulesExecutorData first = newData(PRICE_CLASS_DRL);
    RulesExecutorData second = newData(CONSTANT_DRL);

    assertEquals("premium", run(first, 150.0, "price_class"));
    assertEquals("other rules", run(second, 150.0, "price_class"));
    assertEquals("standard", run(first, 50.0, "price_class"));
  }

  @Test
  void invalidRulesFailAtInit() {
    RulesExecutorData data = new RulesExecutorData();
    data.setRuleString("package broken;\nrule \"No end\"\nwhen\n  eval(true)\nthen\n");

    RuleValidationException e = assertThrows(RuleValidationException.class, data::initializeRules);
    assertFalse(e.getMessages().isEmpty());
  }
}
