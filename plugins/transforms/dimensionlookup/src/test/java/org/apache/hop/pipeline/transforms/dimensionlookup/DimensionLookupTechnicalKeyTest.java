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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * The technical key travels between two descriptions that need not agree: the key column decides
 * what a lookup reads back, while every key generator hands back a Long. Issue #8130 was a cast
 * across that gap, so these cover the conversion that replaced it without needing a database.
 */
class DimensionLookupTechnicalKeyTest {

  /**
   * Issue #8130. A sequence, the table maximum and the driver's generated keys all produce a Long,
   * whatever the key column is. Storing one in a row described by a numeric(38,0) column used to
   * throw ClassCastException.
   */
  @Test
  void aGeneratedLongIsStoredAsTheKeyColumnDescribesIt() throws HopValueException {
    Object stored = DimensionLookup.asType(new ValueMetaBigNumber("dimension_id", 38, 0), 42L);

    assertEquals(BigDecimal.valueOf(42), stored);
  }

  /**
   * The other direction, and the one the rest of the pipeline sees: the output row declares the
   * technical key an Integer, so a key read back from a numeric column has to arrive as one.
   */
  @Test
  void aKeyReadFromANumericColumnIsHandedOnAsTheDeclaredInteger() throws HopValueException {
    Object handedOn =
        DimensionLookup.asType(new ValueMetaInteger("dimension_id"), new BigDecimal("42"));

    assertEquals(42L, handedOn);
  }

  @Test
  void aValueThatAlreadyMatchesIsPassedThroughUntouched() throws HopValueException {
    Long key = 42L;

    assertSame(key, DimensionLookup.asType(new ValueMetaInteger("dimension_id"), key));
  }

  /** The UUID creation method: the key is a String on both sides and must stay one. */
  @Test
  void aUuidKeyIsNotConverted() throws HopValueException {
    String uuid = "00000000-0000-0000-0000-000000000000";

    assertSame(uuid, DimensionLookup.asType(new ValueMetaString("dimension_id", 36, 0), uuid));
  }

  /** The field creation method against a binary key column, which is what 0012-4 exercises. */
  @Test
  void aBinaryKeyIsNotConverted() throws HopValueException {
    byte[] key = new byte[] {1, 2, 3};

    assertSame(key, DimensionLookup.asType(new ValueMetaBinary("dimension_id"), key));
  }

  @Test
  void aMissingKeyStaysMissing() throws HopValueException {
    assertNull(DimensionLookup.asType(new ValueMetaBigNumber("dimension_id", 38, 0), null));
  }

  @Test
  void aValueWithNothingToConvertToIsLeftAlone() throws HopValueException {
    Long key = 42L;

    assertSame(key, DimensionLookup.asType(null, key));
  }

  /**
   * A value Hop has no description for is handed on rather than guessed at: converting it would
   * mean inventing a type for it, and being wrong about that is how this bug started.
   */
  @Test
  void aValueHopCannotDescribeIsLeftAlone() throws HopValueException {
    Object opaque = new Object();

    assertSame(opaque, DimensionLookup.asType(new ValueMetaInteger("dimension_id"), opaque));
  }

  /**
   * Why the conversion has to describe the value by what it is. Asking the key column's own
   * metadata to read a generated Long is the call that threw in issue #8130, so a later
   * simplification back to it would bring the fault back with it.
   */
  @Test
  void theKeyColumnsOwnMetadataCannotReadAGeneratedLong() {
    IValueMeta keyColumn = new ValueMetaBigNumber("dimension_id", 38, 0);

    assertThrows(HopValueException.class, () -> keyColumn.getNativeDataType(42L));
  }
}
