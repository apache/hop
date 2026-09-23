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

package org.apache.hop.uuid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.validation.ColumnValueConstraints;
import org.apache.hop.core.database.validation.ColumnValueError;
import org.apache.hop.core.database.validation.ColumnValueErrorCode;
import org.apache.hop.core.database.validation.ColumnValueValidator;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Database value validation against a column that Hop reads as {@link ValueMetaUuid}, which is what
 * a PostgreSQL uuid column gives it. The validator's own tests can only use a string target, since
 * the UUID value type lives here.
 */
class UuidColumnValueValidationTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  /**
   * A value that is not a UUID fails to convert to the target type before the UUID check sees it.
   * Reported as a conversion failure it carried the converter's wording rather than naming the
   * fault, and INVALID_UUID was unreachable on every database that has the type.
   */
  @Test
  void invalidUuidOnAColumnReadAsUuid() {
    ColumnValueConstraints spec = new ColumnValueConstraints();
    spec.setColumnName("uid");
    spec.setUuid(true);
    spec.setHopType(IValueMeta.TYPE_UUID);
    spec.setTargetValueMeta(new ValueMetaUuid("uid"));
    ValueMetaString streamMeta = new ValueMetaString("uid");

    assertTrue(
        ColumnValueValidator.validate(
                spec, "uid", streamMeta, "550e8400-e29b-41d4-a716-446655440000", false)
            .isEmpty());

    List<ColumnValueError> errors =
        ColumnValueValidator.validate(spec, "uid", streamMeta, "not-a-uuid", false);
    assertEquals(1, errors.size());
    assertEquals(ColumnValueErrorCode.INVALID_UUID, errors.get(0).code());
    assertEquals("column 'uid': invalid UUID; value='not-a-uuid'", errors.get(0).message());
  }
}
