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

package org.apache.hop.pipeline.transforms.fake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import net.datafaker.Faker;
import org.junit.jupiter.api.Test;

class FakerTypeTest {

  /**
   * Every legacy {@code FakerType} constant must still resolve to a DataFaker accessor, so that
   * pipelines created before the DataFaker migration keep working. {@code Crypto} is the single
   * provider DataFaker dropped and is expected to be unavailable.
   */
  @Test
  void legacyTypesResolveToDataFakerAccessors() {
    List<String> unresolved = new ArrayList<>();
    for (FakerType type : FakerType.values()) {
      if (type == FakerType.Crypto) {
        continue;
      }
      String accessor = FakerType.resolveAccessorMethod(type.name());
      try {
        Faker.class.getMethod(accessor);
      } catch (NoSuchMethodException e) {
        unresolved.add(type.name() + " -> " + accessor);
      }
    }
    assertTrue(unresolved.isEmpty(), "Legacy faker types no longer resolve: " + unresolved);
  }

  @Test
  void newAccessorNamesPassThroughUnchanged() {
    // Fields saved by the new dialog store the accessor directly; resolution must be idempotent.
    assertEquals("phoneNumber", FakerType.resolveAccessorMethod("phoneNumber"));
    assertEquals("numberBetween", FakerType.resolveAccessorMethod("numberBetween"));
  }

  @Test
  void getTypeUsingNameMatchesCaseInsensitivelyAndToleratesMisses() {
    assertSame(FakerType.PhoneNumber, FakerType.getTypeUsingName("PhoneNumber"));
    assertSame(FakerType.PhoneNumber, FakerType.getTypeUsingName("phonenumber"));
    assertSame(FakerType.Name, FakerType.getTypeUsingName("NAME"));
    assertNull(FakerType.getTypeUsingName("NoSuchType"));
    assertNull(FakerType.getTypeUsingName(null));
    assertNull(FakerType.getTypeUsingName(""));
  }

  @Test
  void getFakerMethodReturnsTheDataFakerAccessor() {
    assertEquals("phoneNumber", FakerType.PhoneNumber.getFakerMethod());
    assertEquals("date", FakerType.DateAndTime.getFakerMethod());
    assertEquals("elderScrolls", FakerType.Elder.getFakerMethod());
  }

  @Test
  void resolveAccessorMethodFallsBackToTheStoredValueForUnknownTypes() {
    // A value that is neither a legacy enum name nor blank is returned untouched.
    assertEquals("somethingBrandNew", FakerType.resolveAccessorMethod("somethingBrandNew"));
  }
}
