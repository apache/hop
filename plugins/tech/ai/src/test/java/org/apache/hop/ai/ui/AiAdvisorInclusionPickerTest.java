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

package org.apache.hop.ai.ui;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorInclusionChoice;
import org.junit.jupiter.api.Test;

class AiAdvisorInclusionPickerTest {

  @Test
  void mapsDialogIndexesToChoiceIds() {
    List<AiAdvisorInclusionChoice> choices =
        List.of(
            new AiAdvisorInclusionChoice("SRC_ORDERS", "Orders"),
            new AiAdvisorInclusionChoice("SRC_CUSTOMER", "Customer"));
    assertEquals(
        List.of("SRC_ORDERS", "SRC_CUSTOMER"),
        AiAdvisorSessionPane.selectedChoiceIds(choices, new int[] {0, 1}));
    assertEquals(
        List.of("SRC_CUSTOMER"), AiAdvisorSessionPane.selectedChoiceIds(choices, new int[] {1}));
    assertArrayEquals(
        new int[] {1}, AiAdvisorSessionPane.indexesOf(choices, List.of("SRC_CUSTOMER")));
    assertArrayEquals(new String[] {"Orders", "Customer"}, AiAdvisorSessionPane.labelsOf(choices));
  }
}
