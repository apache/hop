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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import java.io.OutputStream;
import java.util.Locale;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.FastValueFormatter;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.util.StreamLogger;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.postgresql.PGConnection;

/** Stores data for the GPBulkLoader transform. */
@SuppressWarnings("java:S1104")
public class PGBulkLoaderData extends BaseTransformData implements ITransformData {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...

  public StreamLogger errorLogger;

  public Process psqlProcess;

  public StreamLogger outputLogger;

  public OutputStream pgOutputStream;

  public byte[] quote;
  public byte[] separator;
  public byte[] newline;

  public PGConnection pgdb;

  public int[] dateFormatChoices;

  public IValueMeta dateMeta;
  public IValueMeta dateTimeMeta;

  /** The row layout the encoding state below was worked out for. */
  IRowMeta encodedRowMeta;

  /** One COPY row, written to the stream in one go. */
  PGCopyRowBuffer rowBuffer;

  /** Reused to render numbers and dates. */
  StringBuilder scratch;

  /** The enclosure as text, and doubled to escape it inside a string. */
  String quoteString;

  String doubledQuoteString;

  /** Per mapped field: formats its dates without SimpleDateFormat, or null. */
  FastValueFormatter[] dateFormatters;

  /**
   * Per mapped field: whether a lazy (binary string) value's bytes can be copied without converting
   * them, and the grouping symbol they must not hold. See {@link
   * PGBulkLoader#lazyBytesGrouping(IValueMeta)}.
   */
  int[] lazyBytesGrouping;

  /** Default constructor. */
  public PGBulkLoaderData() {
    super();

    db = null;

    // Let's use ISO 8601 format. This in unambiguous with PostgreSQL.
    // Locale.ROOT: the Gregorian calendar and ASCII digits whatever the regional settings, which
    // could otherwise bring a Buddhist year or Thai or Arabic-Indic digits.
    dateMeta = new ValueMetaDate("date");
    dateMeta.setConversionMask("yyyy-MM-dd");
    dateMeta.setDateFormatLocale(Locale.ROOT);

    dateTimeMeta = new ValueMetaDate("date");
    // Let's keep milliseconds. Didn't find a way to keep microseconds (max resolution with PG)
    dateTimeMeta.setConversionMask("yyyy-MM-dd HH:mm:ss.SSS");
    dateTimeMeta.setDateFormatLocale(Locale.ROOT);
  }
}
