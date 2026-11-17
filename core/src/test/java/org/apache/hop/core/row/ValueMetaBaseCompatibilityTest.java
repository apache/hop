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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaBaseCompatibilityTest {

  @SuppressWarnings("deprecation")
  @ParameterizedTest
  @ValueSource(ints = {IValueMeta.TYPE_STRING, IValueMeta.TYPE_DATE, IValueMeta.TYPE_INTEGER})
  void legacyConstructorIsAccessibleOutsideItsPackage(final int type) {
    // A direct call from another package guards the API used by already compiled plugins.
    final var valueMeta = new ValueMetaBase("legacyField", type);

    assertEquals("legacyField", valueMeta.getName());
    assertEquals(type, valueMeta.getType());
    assertEquals(-1, valueMeta.getLength());
    assertEquals(type == IValueMeta.TYPE_INTEGER ? 0 : -1, valueMeta.getPrecision());
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, valueMeta.getStorageType());
  }

  @Test
  void legacyConstructorIsDeprecatedWithoutScheduledRemoval() throws NoSuchMethodException {
    final var constructor = ValueMetaBase.class.getConstructor(String.class, int.class);
    final var deprecation = constructor.getAnnotation(Deprecated.class);

    assertNotNull(deprecation);
    assertFalse(deprecation.forRemoval());
  }
}
