/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.cassandraoutput;

import org.junit.Assert;
import org.junit.Test;

public class CassandraOutputMetaTest {
  @Test
  public void validateConvertToSecondsWithNONETTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.NONE;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( -1, value );

  }

  @Test
  public void validateConvertToSecondsWithSecondsTTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.SECONDS;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( 1, value );

  }

  @Test
  public void validateConvertToSecondsWithMinutesTTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.MINUTES;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( 60, value );

  }

  @Test
  public void validateConvertToSecondsWithHOURSTTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.HOURS;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( 3600, value );

  }

  @Test
  public void validateConvertToSecondsWithDAYSTTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.DAYS;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( 86400, value );

  }
}
