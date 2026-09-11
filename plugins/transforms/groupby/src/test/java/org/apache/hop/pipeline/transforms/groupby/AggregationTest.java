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

package org.apache.hop.pipeline.transforms.groupby;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.apache.hop.core.HopEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class AggregationTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void setTypeLabelKeepsNumericTypeInSync() {
    Aggregation aggregation = new Aggregation();
    aggregation.setTypeLabel("SUM");

    assertEquals("SUM", aggregation.getTypeLabel());
    assertEquals(Aggregation.TYPE_GROUP_SUM, aggregation.getType());

    aggregation.setTypeLabel("COUNT_ALL");
    assertEquals("COUNT_ALL", aggregation.getTypeLabel());
    assertEquals(Aggregation.TYPE_GROUP_COUNT_ALL, aggregation.getType());
  }

  @Test
  void constructorResolvesTypeFromLongDescription() {
    Aggregation aggregation =
        new Aggregation(
            "sum_amount",
            "amount",
            Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
            null);

    assertEquals("sum_amount", aggregation.getField());
    assertEquals("amount", aggregation.getSubject());
    assertEquals(Aggregation.TYPE_GROUP_SUM, aggregation.getType());
    assertEquals("SUM", aggregation.getTypeLabel());
  }

  @Test
  void cloneCreatesIndependentCopy() {
    Aggregation original =
        new Aggregation(
            "moving_avg",
            "amount",
            Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_MOVING_AVERAGE),
            "3",
            "ts");

    Aggregation copy = original.clone();

    assertNotSame(original, copy);
    assertEquals(original, copy);

    copy.setField("other");
    assertNotEquals(original.getField(), copy.getField());
  }

  @Test
  void equalsAndHashCodeConsiderAllFields() {
    Aggregation left =
        new Aggregation(
            "sum_amount",
            "amount",
            Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
            null);
    Aggregation right =
        new Aggregation(
            "sum_amount",
            "amount",
            Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
            null);
    Aggregation different =
        new Aggregation(
            "cnt",
            "amount",
            Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_COUNT_ALL),
            null);

    assertEquals(left, right);
    assertEquals(left.hashCode(), right.hashCode());
    assertNotEquals(left, different);
  }
}
