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

package org.apache.hop.pipeline.transforms.databaselookup.readallcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.BitSet;
import org.junit.jupiter.api.Test;

class SearchingContextTest {

  @Test
  void returnsClearWorkingSet() {
    SearchingContext ctx = new SearchingContext();
    ctx.init(4);
    ctx.getWorkingSet().set(1);
    assertEquals(-1, ctx.getWorkingSet().nextSetBit(0), "Should return cleared object");
  }

  @Test
  void intersectionDetectsBecomingEmpty() {

    SearchingContext ctx = new SearchingContext();
    ctx.init(4);

    BitSet set = ctx.getWorkingSet();
    set.set(1);
    set.set(2);
    ctx.intersect(set, false);
    assertFalse(ctx.isEmpty());

    set = ctx.getWorkingSet();
    ctx.intersect(set, false);
    assertTrue(ctx.isEmpty(), "Expected to be empty");
  }
}
