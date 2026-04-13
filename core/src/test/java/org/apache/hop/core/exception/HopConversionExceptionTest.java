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

package org.apache.hop.core.exception;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class HopConversionExceptionTest {

  @Test
  void constructorStoresCausesFieldsAndRow() {
    List<Exception> causes = Collections.singletonList(new NumberFormatException("bad"));
    List<IValueMeta> fields = Collections.singletonList(new ValueMetaString("f1"));
    Object[] row = new Object[] {null, "ok"};
    HopConversionException ex =
        new HopConversionException("conversion failed", causes, fields, row);
    assertEquals("conversion failed", ex.getSuperMessage());
    assertEquals(causes, ex.getCauses());
    assertEquals(fields, ex.getFields());
    assertArrayEquals(row, ex.getRowData());
    assertTrue(ex.getMessage().contains("conversion failed"));
  }

  @Test
  void defaultConstructor() {
    HopConversionException ex = new HopConversionException();
    assertNull(ex.getCauses());
    assertNull(ex.getFields());
    assertNull(ex.getRowData());
  }
}
