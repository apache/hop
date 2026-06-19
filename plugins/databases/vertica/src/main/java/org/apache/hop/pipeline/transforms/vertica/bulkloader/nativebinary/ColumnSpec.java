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

package org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

public class ColumnSpec {
  private static final byte BYTE_ZERO = (byte) 0;
  private static final byte BYTE_ONE = (byte) 1;
  private static final byte BYTE_SPACE = (byte) 0x20;

  public enum ConstantWidthType {
    INTEGER_8(ColumnType.INTEGER, 1),
    INTEGER_16(ColumnType.INTEGER, 2),
    INTEGER_32(ColumnType.INTEGER, 4),
    INTEGER_64(ColumnType.INTEGER, 8),
    BOOLEAN(ColumnType.BOOLEAN, 1),
    FLOAT(ColumnType.FLOAT, 8),
    DATE(ColumnType.DATE, 8),
    TIME(ColumnType.TIME, 8),
    TIMETZ(ColumnType.TIMETZ, 8),
    TIMESTAMP(ColumnType.TIMESTAMP, 8),
    TIMESTAMPTZ(ColumnType.TIMESTAMPTZ, 8),
    INTERVAL(ColumnType.INTERVAL, 8);

    private final ColumnType type;
    private final int bytes;

    private ConstantWidthType(ColumnType type, int bytes) {
      this.type = type;
      this.bytes = bytes;
    }
  }

  public enum VariableWidthType {
    VARCHAR(ColumnType.VARCHAR),
    VARBINARY(ColumnType.VARBINARY);

    private final ColumnType type;
    private final int bytes = -1;

    private VariableWidthType(ColumnType type) {
      this.type = type;
    }
  }

  public enum UserDefinedWidthType {
    CHAR(ColumnType.CHAR),
    BINARY(ColumnType.BINARY);

    private final ColumnType type;

    private UserDefinedWidthType(ColumnType type) {
      this.type = type;
    }
  }

  public enum PrecisionScaleWidthType {
    NUMERIC(ColumnType.NUMERIC);

    private final ColumnType type;

    private PrecisionScaleWidthType(ColumnType type) {
      this.type = type;
    }
  }

  public final ColumnType type;
  public int bytes;
  public final int scale;
  private final int maxLength;
  private CharBuffer charBuffer;
  private CharsetEncoder charEncoder;
  private ByteBuffer mainBuffer;

  private final GregorianCalendar calendarLocalTZ = new GregorianCalendar();
  private final Calendar calendarUTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  /**
   * In Vertica, dates are stored as differences with Jan 01 2000 00:00:00. This is Julian Day
   * Number of it (must be equal to 2451545)
   */
  private static final int BASE_DATE_JDN = computeJdn(2000, 1, 1);

  /** The timestamp of Jan 01 2000 00:00:00 in UTC timezone */
  private static final long BASE_DATE_UTC_MILLIS;

  static {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.clear();
    calendar.set(2000, 0, 1, 0, 0, 0);
    BASE_DATE_UTC_MILLIS = calendar.getTimeInMillis();
  }

  public ColumnSpec(PrecisionScaleWidthType precisionScaleWidthType, int precision, int scale) {
    this.type = precisionScaleWidthType.type;
    this.bytes = -1; // NUMERIC is encoded as VARCHAR (length = -1)
    this.scale = scale;
    this.maxLength = precision;
  }

  public ColumnSpec(UserDefinedWidthType userDefinedWidthType, int bytes) {
    this.type = userDefinedWidthType.type;
    this.bytes = bytes;
    this.scale = 0;
    this.maxLength = bytes;
  }

  public ColumnSpec(ConstantWidthType constantWidthType) {
    this.type = constantWidthType.type;
    this.bytes = constantWidthType.bytes;
    this.scale = 0;
    this.maxLength = constantWidthType.bytes;
  }

  public ColumnSpec(VariableWidthType variableWidthType, int maxlenght) {
    this.type = variableWidthType.type;
    this.bytes = variableWidthType.bytes;
    this.scale = 0;
    this.maxLength = maxlenght;
  }

  public void setCharBuffer(CharBuffer charBuffer) {
    this.charBuffer = charBuffer;
  }

  public void setCharEncoder(CharsetEncoder charEncoder) {
    this.charEncoder = charEncoder;
  }

  public void setMainBuffer(ByteBuffer buffer) {
    this.mainBuffer = buffer;
  }

  public void encode(IValueMeta valueMeta, Object value) throws HopValueException {
    if (value == null || valueMeta == null || valueMeta.getNativeDataType(value) == null) {
      return;
    }

    int prevPosition, length, sizePosition;
    byte[] inputBinary;
    long milliSeconds;

    switch (this.type) {
      case BINARY:
        inputBinary = valueMeta.getBinaryString(value);
        length = inputBinary.length;
        this.mainBuffer.put(inputBinary);
        for (int i = 0; i < (this.bytes - length); i++) {
          this.mainBuffer.put(BYTE_ZERO);
        }
        break;
      case BOOLEAN:
        this.mainBuffer.put(valueMeta.getBoolean(value) ? BYTE_ONE : BYTE_ZERO);
        break;
      case CHAR:
        this.charBuffer.clear();
        this.charEncoder.reset();
        this.charBuffer.put(valueMeta.getString(value));
        this.charBuffer.flip();
        prevPosition = this.mainBuffer.position();
        this.charEncoder.encode(this.charBuffer, this.mainBuffer, true);
        int encodedLength = this.mainBuffer.position() - prevPosition;
        for (int i = 0; i < (this.bytes - encodedLength); i++) {
          this.mainBuffer.put(BYTE_SPACE);
        }
        break;
      case DATE:
        // 64-bit integer in little-endian format containing the Julian day
        // since Jan 01 2000 (J2451545)
        calendarLocalTZ.setTime(valueMeta.getDate(value));
        int julianEnd = computeJdn(calendarLocalTZ);
        this.mainBuffer.putLong(julianEnd - BASE_DATE_JDN);
        break;
      case FLOAT:
        this.mainBuffer.putDouble(valueMeta.getNumber(value));
        break;
      case INTEGER:
        switch (this.bytes) {
          case 1:
            this.mainBuffer.put(valueMeta.getInteger(value).byteValue());
            break;
          case 2:
            this.mainBuffer.putShort(valueMeta.getInteger(value).shortValue());
            break;
          case 4:
            this.mainBuffer.putInt(valueMeta.getInteger(value).intValue());
            break;
          case 8:
            this.mainBuffer.putLong(valueMeta.getInteger(value));
            break;
          default:
            throw new IllegalArgumentException("Invalid byte size for Integer type");
        }
        break;
      case INTERVAL:
        this.mainBuffer.putLong(valueMeta.getInteger(value));
        break;
      case TIME:
        // 64-bit integer in little-endian format containing the number of microseconds since
        // midnight in the UTC time
        // zone.
        // We actually use the local time instead of the UTC time because UTC time was giving wrong
        // results.
        calendarLocalTZ.setTime(valueMeta.getDate(value));
        milliSeconds =
            TimeUnit.HOURS.toMillis(calendarLocalTZ.get(Calendar.HOUR_OF_DAY))
                + TimeUnit.MINUTES.toMillis(calendarLocalTZ.get(Calendar.MINUTE))
                + TimeUnit.SECONDS.toMillis(calendarLocalTZ.get(Calendar.SECOND))
                + calendarLocalTZ.get(Calendar.MILLISECOND);
        this.mainBuffer.putLong(TimeUnit.MILLISECONDS.toMicros(milliSeconds));
        break;
      case TIMETZ:
        // HP Vertica Documentation. Software Version: 7.1.x (Document Release Date: 3/31/2015)
        // 64-bit value where
        //  - Upper 40 bits contain the number of microseconds since midnight
        //  - Lower 24 bits contain time zone as the UTC offset in microseconds calculated as
        // follows: Time zone is
        //    logically from -24hrs to +24hrs from UTC. Instead it is represented here as a number
        // between 0hrs to
        //    48hrs. Therefore, 24hrs should be added to the actual time zone to calculate it.

        // AK: there is an obvious mistake in the description above
        //        48 hours is 48*3600000=172800000 microseconds
        //        24 bits can store 2^24= 16777216 values
        // Here is what another doc says
        // (https://my.vertica.com/docs/5.0/SDK/html/_timestamp_u_dx_shared_8h.htm#a143e616e0854a9dcded5dd314162e5dd):
        // typedef int64 TimeTzADT
        //    Represents time within a day in a timezone
        //    The value in TimeADT consists of 2 parts:
        //
        //    1. The lower 24 bits (defined as ZoneFieldWidth) contains the timezone plus 24 hours,
        // specified in
        // seconds SQL-2008 limits the timezone itself to range between +/-14 hours

        // We can store either local time and local time zone's offset or convert local time to UTC
        // and the offset is
        // constant in this case. The latter approach is implemented below

        calendarUTC.setTime(valueMeta.getDate(value));
        milliSeconds =
            TimeUnit.HOURS.toMillis(calendarUTC.get(Calendar.HOUR_OF_DAY))
                + TimeUnit.MINUTES.toMillis(calendarUTC.get(Calendar.MINUTE))
                + TimeUnit.SECONDS.toMillis(calendarUTC.get(Calendar.SECOND))
                + calendarUTC.get(Calendar.MILLISECOND);
        final long utcOffsetInSeconds = 24 * 3600;
        this.mainBuffer.putLong(
            ((TimeUnit.MILLISECONDS.toMicros(milliSeconds)) << 24) + utcOffsetInSeconds);
        break;
      case TIMESTAMP:
        // 64-bit integer in little-endian format containing the number of microseconds since Julian
        // day: Jan 01 2000
        // 00:00:00.
        calendarLocalTZ.setTime(valueMeta.getDate(value));
        milliSeconds = computeDiffInMillisDisrespectingDst(calendarLocalTZ);
        this.mainBuffer.putLong(TimeUnit.MILLISECONDS.toMicros(milliSeconds));
        break;
      case TIMESTAMPTZ:
        // A 64-bit integer in little-endian format containing the number of microseconds since
        // Julian day: Jan 01 2000
        // 00:00:00 in the UTC timezone.
        calendarUTC.setTime(valueMeta.getDate(value));
        milliSeconds = calendarUTC.getTimeInMillis() - BASE_DATE_UTC_MILLIS;
        this.mainBuffer.putLong(TimeUnit.MILLISECONDS.toMicros(milliSeconds));
        break;
      case VARBINARY:
        sizePosition = this.mainBuffer.position();
        this.mainBuffer.putInt(0);
        prevPosition = this.mainBuffer.position();
        this.mainBuffer.put(valueMeta.getBinaryString(value));
        this.mainBuffer.putInt(sizePosition, this.mainBuffer.position() - prevPosition);
        this.bytes = this.mainBuffer.position() - sizePosition;
        break;
      case NUMERIC:
        // Numeric is encoded as VARCHAR. COPY statement uses is as a FILLER column for Vertica
        // itself
        // to convert into internal NUMERIC data format.
      case VARCHAR:
        this.charBuffer.clear();
        this.charEncoder.reset();
        this.charBuffer.put(valueMeta.getString(value));
        this.charBuffer.flip();
        sizePosition = this.mainBuffer.position();
        this.mainBuffer.putInt(0);
        prevPosition = this.mainBuffer.position();
        this.charEncoder.encode(this.charBuffer, this.mainBuffer, true);
        int dataLength = this.mainBuffer.position() - prevPosition;
        this.mainBuffer.putInt(sizePosition, dataLength);
        this.bytes = this.mainBuffer.position() - sizePosition;
        break;

      default:
        throw new IllegalArgumentException("Invalid ColumnType");
    }
  }

  /**
   * Returns the Julian Day Number from a Gregorian calendar.
   *
   * @param calendar gregorian calendar
   * @return the Julian day number
   */
  private static int computeJdn(GregorianCalendar calendar) {
    // Note: Calendar.JANUARY == 0, whereas it is expected to be 1
    return computeJdn(
        calendar.get(Calendar.YEAR),
        calendar.get(Calendar.MONTH) + 1,
        calendar.get(Calendar.DAY_OF_MONTH));
  }

  /**
   * Returns the Julian Day Number from a Gregorian day. The algorithm is taken from Wikipedia: <a
   * href="http://en.wikipedia.org/wiki/Julian_day">Julian Day</a>
   *
   * @param year year, e.g. 2000
   * @param month month number: 1 for Jan, 2 for Feb, etc.
   * @param day day of month, starting from 1
   * @return the Julian day number
   */
  private static int computeJdn(int year, int month, int day) {
    int a = (14 - month) / 12;
    int y = year + 4800 - a;
    int m = month + 12 * a - 3;
    // since the order number is computed, there's no need to use floating point types
    return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
  }

  /**
   * Returns the difference between <tt>calendar</tt> and Vertica's base date (<tt>Jan 01 2000
   * 00:00:00</tt>) in milliseconds.
   *
   * @param calendar desired moment of time
   * @return the values delta in milliseconds
   */
  private static long computeDiffInMillisDisrespectingDst(GregorianCalendar calendar) {
    // The goal is to compute the difference between two moments of time
    // We can use TimeZone.inDaylightTime() or calculate a Julian Day Number
    // I prefer the second approach
    int days = computeJdn(calendar) - BASE_DATE_JDN;
    int hours = calendar.get(Calendar.HOUR_OF_DAY);
    int minutes = calendar.get(Calendar.MINUTE);
    int seconds = calendar.get(Calendar.SECOND);
    long millis = calendar.get(Calendar.MILLISECOND);
    return TimeUnit.DAYS.toMillis(days)
        + TimeUnit.HOURS.toMillis(hours)
        + TimeUnit.MINUTES.toMillis(minutes)
        + TimeUnit.SECONDS.toMillis(seconds)
        + millis;
  }

  /**
   * @return the maxLenght
   */
  public int getMaxLength() {
    return maxLength;
  }
}
