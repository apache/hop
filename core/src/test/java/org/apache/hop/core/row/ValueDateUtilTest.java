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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;


public class ValueDateUtilTest {
  private TimeZone defTimeZone;
  private TimeZone defUserTimezone;

  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @Before
  public void setUp() {
    defUserTimezone = TimeZone.getTimeZone( System.getProperty( "user.timezone" ) );
    defTimeZone = java.util.TimeZone.getDefault();
    System.setProperty( "user.timezone", "UTC" );
    TimeZone.setDefault( null );
  }

  @After
  public void tearDown() {
    System.clearProperty( Const.HOP_COMPATIBILITY_CALCULATION_TIMEZONE_DECOMPOSITION );
    System.setProperty( "user.timezone", defUserTimezone.getID() );
    TimeZone.setDefault( defTimeZone );
  }

  @Test
  public void shouldCalculateHourOfDayUsingValueMetasTimeZoneByDefault() throws HopValueException {
    Calendar date = Calendar.getInstance();
    date.setTimeInMillis( 1454313600000L ); // 2016-07-01 08:00:00 UTC
    IValueMeta valueMetaDate = new ValueMetaDate();
    valueMetaDate.setDateFormatTimeZone( TimeZone.getTimeZone( "CET" ) ); // UTC +1
    long offsetCET = (long) TimeZone.getTimeZone( "CET" ).getRawOffset() / 3600000;

    Object hourOfDayCET = ValueDataUtil.hourOfDay( valueMetaDate, date.getTime() );

    assertEquals( 8L + offsetCET, hourOfDayCET );
  }

  @Test
  public void shouldCalculateHourOfDayUsingLocalTimeZoneIfPropertyIsSet() throws HopValueException {
    Calendar date = Calendar.getInstance();
    date.setTimeInMillis( 1454313600000L ); // 2016-07-01 08:00:00 UTC
    IValueMeta valueMetaDate = new ValueMetaDate();
    valueMetaDate.setDateFormatTimeZone( TimeZone.getTimeZone( "CET" ) ); // UTC +1
    long offsetLocal = (long) TimeZone.getDefault().getRawOffset() / 3600000;
    System.setProperty( Const.HOP_COMPATIBILITY_CALCULATION_TIMEZONE_DECOMPOSITION, "true" );

    Object hourOfDayLocal = ValueDataUtil.hourOfDay( valueMetaDate, date.getTime() );

    assertEquals( 8L + offsetLocal, hourOfDayLocal );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_JAN() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1230768000000L ); // 2009-01-01 00:00:00
    endDate.setTimeInMillis( 1233360000000L ); // 2009-01-31 00:00:00
    Object workingDayOfJAN = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in JAN ", 22L, workingDayOfJAN );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_FEB() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1233446400000L ); // 2009-02-01 00:00:00
    endDate.setTimeInMillis( 1235779200000L ); // 2009-02-28 00:00:00
    Object workingDayOfFEB = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in FEB ", 20L, workingDayOfFEB );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_MAR() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1235865600000L ); // 2009-03-01 00:00:00
    endDate.setTimeInMillis( 1238457600000L ); // 2009-03-31 00:00:00
    Object workingDayOfMAR = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in MAR ", 22L, workingDayOfMAR );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_APR() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1238544000000L ); // 2009-04-01 00:00:00
    endDate.setTimeInMillis( 1241049600000L ); // 2009-04-30 00:00:00
    Object workingDayOfAPR = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in APR ", 22L, workingDayOfAPR );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_MAY() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1241136000000L ); // 2009-05-01 00:00:00
    endDate.setTimeInMillis( 1243728000000L );     // 2009-05-31 00:00:00
    Object workingDayOfMAY = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in MAY ", 21L, workingDayOfMAY );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_JUN() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1243814400000L ); // 2009-06-01 00:00:00
    endDate.setTimeInMillis( 1246320000000L );     // 2009-06-30 00:00:00
    Object workingDayOfJUN = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in JUN ", 22L, workingDayOfJUN );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_JUL() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1246406400000L ); // 2009-07-01 00:00:00
    endDate.setTimeInMillis( 1248998400000L );     // 2009-07-31 00:00:00
    Object workingDayOfJUL = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in JUL ", 23L, workingDayOfJUL );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_AUG() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1249084800000L ); // 2009-08-01 00:00:00
    endDate.setTimeInMillis( 1251676800000L );     // 2009-08-31 00:00:00
    Object workingDayOfAUG = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in AUG ", 21L, workingDayOfAUG );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_SEP() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1251763200000L ); // 2009-09-01 00:00:00
    endDate.setTimeInMillis( 1254268800000L );     // 2009-09-30 00:00:00
    Object workingDayOfSEP = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in SEP ", 22L, workingDayOfSEP );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_OCT() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1254355200000L ); // 2009-10-01 00:00:00
    endDate.setTimeInMillis( 1256947200000L );     // 2009-10-31 00:00:00
    Object workingDayOfOCT = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in OCT ", 22L, workingDayOfOCT );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_NOV() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1257033600000L ); // 2009-11-01 00:00:00
    endDate.setTimeInMillis( 1259539200000L );     // 2009-11-30 00:00:00
    Object workingDayOfNOV = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in NOV ", 21L, workingDayOfNOV );
  }

  @Test
  public void shouldCalculateDateWorkingDiff_DEC() throws HopValueException {
    IValueMeta metaA = new ValueMetaDate();
    IValueMeta metaB = new ValueMetaDate();
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    startDate.setTimeInMillis( 1259625600000L ); // 2009-12-01 00:00:00
    endDate.setTimeInMillis( 1262217600000L );     // 2009-12-31 00:00:00
    Object workingDayOfDEC = ValueDataUtil.DateWorkingDiff( metaA, endDate.getTime(), metaB, startDate.getTime() );
    assertEquals( "Working days count in DEC ", 23L, workingDayOfDEC );
  }

}
