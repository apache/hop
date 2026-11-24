/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.pgbulkloader;

//
// The "designer" notes of the PostgreSQL bulkloader:
// ----------------------------------------------
//
// Let's see how fast we can push data down the tube with the use of COPY FROM STDIN
//
//

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormatSymbols;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Hex;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.FastValueFormatter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;

/** Performs a bulk load to a postgres table. */
public class PGBulkLoader extends BaseTransform<PGBulkLoaderMeta, PGBulkLoaderData> {

  private static final Class<?> PKG = PGBulkLoaderMeta.class;

  /**
   * The encoding of every byte sent to the COPY. The COPY command declares it, so the session's
   * client_encoding does not matter.
   */
  private static final Charset COPY_ENCODING = StandardCharsets.UTF_8;

  /**
   * A conversion mask that only places digits, a decimal point, grouping and an exponent. Text read
   * with it means the same to PostgreSQL, as long as the decimal symbol is a point and the text has
   * no grouping symbols.
   */
  private static final Pattern PLAIN_NUMBER_MASK = Pattern.compile("[#0,]*(\\.[#0]*)?(E0+)?");

  /** A lazy value's bytes need converting. */
  private static final int CONVERT_LAZY_BYTES = -2;

  /** A lazy value's bytes can be copied when they are ASCII. */
  private static final int NO_GROUPING_SYMBOL = -1;

  private PGCopyOutputStream pgCopyOut;

  public PGBulkLoader(
      TransformMeta transformMeta,
      PGBulkLoaderMeta meta,
      PGBulkLoaderData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Get the contents of the control file as specified in the meta object
   *
   * @return a string containing the control file contents
   */
  public String getCopyCommand() throws HopException {
    DatabaseMeta dm = getPipelineMeta().findDatabase(meta.getConnection(), variables);

    StringBuilder contents = new StringBuilder(500);

    String tableName =
        dm.getQuotedSchemaTableCombination(this, meta.getSchemaName(), meta.getTableName());

    // Create a Postgres / Greenplum COPY string for use with a psql client
    contents.append("COPY ");
    // Table name

    contents.append(tableName);

    // Names of columns

    contents.append(" ( ");

    List<PGBulkLoaderMappingMeta> mapping = meta.getMappings();

    if (Utils.isEmpty(mapping)) {
      throw new HopException("No fields defined to load to database");
    }

    for (int i = 0; i < mapping.size(); i++) {
      if (i != 0) {
        contents.append(", ");
      }
      contents.append(dm.quoteField(mapping.get(i).getFieldTable()));
    }

    contents.append(" ) ");

    // The "FROM" filename
    contents.append(" FROM STDIN"); // FIFO file

    // The "FORMAT" clause. The rows are always sent in UTF-8 and the ENCODING option says so,
    // whatever the session's client_encoding (PostgreSQL 9.1 and later).
    contents
        .append(" WITH (FORMAT csv, DELIMITER '")
        .append(resolve(meta.getDelimiter()))
        .append("', QUOTE '")
        .append(resolve(meta.getEnclosure()))
        .append("', ENCODING 'UTF8')");
    contents.append(";").append(Const.CR);

    return contents.toString();
  }

  private void doCopy(PGBulkLoaderMeta meta, boolean wait) throws HopException {
    data.db = getDatabase(this, meta);
    String copyCmd = getCopyCommand();
    try {
      connect();

      processTruncate();

      if (isBasic()) {
        logBasic("Launching command: " + copyCmd);
      }
      // unwrap: in a transactional pipeline the connection is a shared wrapper (see
      // SharedConnectionLock), not the driver's own connection.
      pgCopyOut =
          new PGCopyOutputStream(data.db.getConnection().unwrap(PGConnection.class), copyCmd);

    } catch (Exception ex) {
      throw new HopException("Error while preparing the COPY " + copyCmd, ex);
    }
  }

  @VisibleForTesting
  Database getDatabase(ILoggingObject parentObject, PGBulkLoaderMeta pgBulkLoaderMeta) {
    DatabaseMeta dbMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
    // If dbNameOverride is present, clone the origin db meta and override the DB name
    String dbNameOverride = resolve(pgBulkLoaderMeta.getDbNameOverride());
    if (!Utils.isEmpty(dbNameOverride)) {
      dbMeta.setDBName(dbNameOverride.trim());
      if (isDebug()) {
        logDebug("DB name overridden to the value: " + dbNameOverride);
      }
    }
    return new Database(parentObject, variables, dbMeta);
  }

  void connect() throws HopException {
    data.db.connect();
  }

  void processTruncate() throws Exception {
    Connection connection = data.db.getConnection();

    String loadAction = resolve(meta.getLoadAction());

    if (loadAction.equalsIgnoreCase("truncate")) {
      DatabaseMeta dm = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      String tableName =
          dm.getQuotedSchemaTableCombination(this, meta.getSchemaName(), meta.getTableName());
      if (isBasic()) {
        logBasic("Launching command: " + "TRUNCATE " + tableName);
      }

      Statement statement = connection.createStatement();

      try {
        statement.executeUpdate("TRUNCATE " + tableName);
      } catch (Exception ex) {
        throw new HopException("Error while truncating " + tableName, ex);
      } finally {
        statement.close();
      }
    }
  }

  @Override
  public boolean processRow() throws HopException {

    try {
      Object[] r = getRow(); // Get row from input rowset & set row busy!

      if (r == null) { // no more input to be expected...

        setOutputDone();

        // Close the output stream...
        // will be null if no records (empty stream)
        if (data != null && pgCopyOut != null) {
          pgCopyOut.flush();
          pgCopyOut.endCopy();
          pgCopyOut.close();
          pgCopyOut = null;
        }
        if (data != null && data.db != null) {
          data.db.disconnect();
          data.db = null;
        }

        return false;
      }

      if (first) {
        first = false;

        // Cache field indexes.
        //
        data.keynrs = new int[meta.getMappings().size()];
        for (int i = 0; i < data.keynrs.length; i++) {
          data.keynrs[i] =
              getInputRowMeta().indexOfValue(meta.getMappings().get(i).getFieldStream());
        }

        // execute the copy statement... pgCopyOut is setup there
        //
        doCopy(meta, true);

        // Write rows of data hereafter...
        //
      }

      writeRowToPostgres(getInputRowMeta(), r);

      putRow(getInputRowMeta(), r);
      incrementLinesOutput();

      return true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "GPBulkLoader.Log.ErrorInTransform"), e);
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }
  }

  /**
   * Encodes a boolean for PostgreSQL COPY text format. {@code t}/{@code f} are accepted; literals
   * like {@code 1.0} from a numeric conversion are not.
   */
  @VisibleForTesting
  static byte[] booleanFieldBytesForPgCopyText(
      IValueMeta valueMeta, Object valueData, Charset charset) throws HopValueException {
    Boolean bool = valueMeta.getBoolean(valueData);
    if (bool == null) {
      return null;
    }
    return (bool ? "t" : "f").getBytes(charset);
  }

  /**
   * Encodes binary data for PostgreSQL COPY text/CSV format. {@code bytea} accepts the hex form
   * {@code \x} followed by two hex digits per byte.
   */
  @VisibleForTesting
  static byte[] binaryFieldBytesForPgCopyText(
      IValueMeta valueMeta, Object valueData, Charset charset) throws HopValueException {
    byte[] bytes = valueMeta.getBinary(valueData);
    if (bytes == null) {
      return null;
    }
    return ("\\x" + Hex.encodeHexString(bytes)).getBytes(charset);
  }

  @VisibleForTesting
  void writeRowToPostgres(IRowMeta rowMeta, Object[] r) throws HopException {

    try {
      // The row is encoded into one reusable buffer and handed to the copy stream in one write.
      // The bytes are the same as writing every value on its own.
      //
      if (data.rowBuffer == null || data.encodedRowMeta != rowMeta) {
        prepareRowEncoding(rowMeta);
      }
      PGCopyRowBuffer buf = data.rowBuffer;
      StringBuilder text = data.scratch;
      buf.reset();

      for (int i = 0; i < data.keynrs.length; i++) {
        if (i > 0) {
          // Write a separator
          //
          buf.append(data.separator);
        }

        int index = data.keynrs[i];
        IValueMeta valueMeta = rowMeta.getValueMeta(index);
        Object valueData = r[index];

        if (valueData != null) {
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_STRING:
              buf.append(data.quote);

              // No longer dump the bytes for a Lazy Conversion
              // We need to escape the quote characters in every string
              String string = valueMeta.getString(valueData);
              if (!data.quoteString.isEmpty() && string.contains(data.quoteString)) {
                string = string.replace(data.quoteString, data.doubledQuoteString);
              }
              buf.append(string);

              buf.append(data.quote);
              break;
            case IValueMeta.TYPE_INTEGER:
              if (!appendLazyBytes(buf, i, valueMeta, valueData)) {
                Long integer = valueMeta.getInteger(valueData);
                if (integer != null) {
                  text.setLength(0);
                  buf.append(text.append(integer.longValue()));
                }
              }
              break;
            case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP:
              appendDate(buf, text, i, valueMeta, valueData);
              break;
            case IValueMeta.TYPE_BOOLEAN:
              byte[] boolBytes =
                  booleanFieldBytesForPgCopyText(valueMeta, valueData, COPY_ENCODING);
              if (boolBytes != null) {
                buf.append(boolBytes);
              }
              break;
            case IValueMeta.TYPE_NUMBER:
              if (!appendLazyBytes(buf, i, valueMeta, valueData)) {
                Double number = valueMeta.getNumber(valueData);
                if (number != null) {
                  text.setLength(0);
                  // the same digits as Double.toString(number)
                  buf.append(text.append(number.doubleValue()));
                }
              }
              break;
            case IValueMeta.TYPE_BIGNUMBER:
              if (!appendLazyBytes(buf, i, valueMeta, valueData)) {
                BigDecimal big = valueMeta.getBigNumber(valueData);
                if (big != null) {
                  buf.append(big.toString());
                }
              }
              break;
            case IValueMeta.TYPE_BINARY:
              byte[] binaryBytes =
                  binaryFieldBytesForPgCopyText(valueMeta, valueData, COPY_ENCODING);
              if (binaryBytes != null) {
                buf.append(binaryBytes);
              }
              break;
            default:

              // UUID supports
              if (valueMeta.getType() == ValueMetaFactory.getIdForValueMeta("UUID")) {
                if (!appendLazyBytes(buf, i, valueMeta, valueData)) {
                  String s = valueMeta.getString(valueData);
                  if (s != null) {
                    buf.append(s);
                  }
                }
              } else {
                throw new HopException(
                    "PGBulkLoader doesn't handle the type " + valueMeta.getTypeDesc());
              }
          }
        }
      }

      // Now write a newline
      //
      buf.append(data.newline);
      buf.writeTo(pgCopyOut);
    } catch (Exception e) {
      throw new HopException("Error serializing rows of data to the COPY command", e);
    }
  }

  /**
   * Works out, once per row layout, the reusable row buffer and for every date column whether it
   * can be formatted without SimpleDateFormat (with exactly the same text).
   */
  private void prepareRowEncoding(IRowMeta rowMeta) {
    data.rowBuffer = new PGCopyRowBuffer(COPY_ENCODING);
    data.scratch = new StringBuilder(64);
    data.quoteString = new String(data.quote);
    data.doubledQuoteString = data.quoteString + data.quoteString;
    data.dateFormatters = new FastValueFormatter[data.keynrs.length];
    data.lazyBytesGrouping = new int[data.keynrs.length];
    for (int i = 0; i < data.keynrs.length; i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(data.keynrs[i]);
      data.lazyBytesGrouping[i] = lazyBytesGrouping(valueMeta);
      if (valueMeta.getType() != IValueMeta.TYPE_DATE
          && valueMeta.getType() != IValueMeta.TYPE_TIMESTAMP) {
        continue;
      }
      switch (data.dateFormatChoices[i]) {
        case PGBulkLoaderMeta.NR_DATE_MASK_PASS_THROUGH:
          data.dateFormatters[i] = FastValueFormatter.forValueMeta(valueMeta);
          break;
        case PGBulkLoaderMeta.NR_DATE_MASK_DATE:
          data.dateFormatters[i] = FastValueFormatter.forValueMeta(data.dateMeta);
          break;
        case PGBulkLoaderMeta.NR_DATE_MASK_DATETIME:
          data.dateFormatters[i] = FastValueFormatter.forValueMeta(data.dateTimeMeta);
          break;
        default:
          break;
      }
    }
    data.encodedRowMeta = rowMeta;
  }

  /**
   * Copies a lazy (binary string) value as the bytes it was read as, when PostgreSQL reads them as
   * Hop does: ASCII text, without grouping symbols, of a field {@link #lazyBytesGrouping} allows.
   *
   * @return false when the value is not lazy or its bytes need converting
   */
  private boolean appendLazyBytes(
      PGCopyRowBuffer buf, int i, IValueMeta valueMeta, Object valueData) {
    int grouping = data.lazyBytesGrouping[i];
    if (grouping == CONVERT_LAZY_BYTES || !valueMeta.isStorageBinaryString()) {
      return false;
    }
    byte[] bytes = (byte[]) valueData;
    for (byte b : bytes) {
      if (b < 0 || b == grouping) {
        return false;
      }
    }
    buf.append(bytes);
    return true;
  }

  /**
   * Whether the bytes of a lazy number or UUID field can be copied as they were read. They can when
   * the storage encoding keeps ASCII as ASCII and, for numbers, the conversion mask is plain and
   * the decimal symbol is a point. Such text means the same to PostgreSQL as to Hop, unless it
   * holds the grouping symbol, which is returned.
   *
   * @return the grouping symbol (or {@link #NO_GROUPING_SYMBOL}), or {@link #CONVERT_LAZY_BYTES}
   */
  static int lazyBytesGrouping(IValueMeta valueMeta) {
    IValueMeta storage = valueMeta.getStorageMetadata();
    if (!valueMeta.isStorageBinaryString() || storage == null) {
      return CONVERT_LAZY_BYTES;
    }
    if (!isAsciiCompatible(storage.getStringEncoding())) {
      return CONVERT_LAZY_BYTES;
    }
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER:
        break;
      default:
        // UUID: its text has no format of its own
        return NO_GROUPING_SYMBOL;
    }
    // The storage metadata parses the text, with its conversion metadata's format if it has one.
    IValueMeta format =
        storage.getConversionMetadata() != null ? storage.getConversionMetadata() : storage;
    String mask = format.getConversionMask();
    if (!Utils.isEmpty(mask) && !PLAIN_NUMBER_MASK.matcher(mask).matches()) {
      return CONVERT_LAZY_BYTES;
    }
    // An empty symbol leaves the regional settings' one in place (see getDecimalFormat()).
    DecimalFormatSymbols regional =
        DecimalFormatSymbols.getInstance(Locale.getDefault(Locale.Category.FORMAT));
    String decimal = format.getDecimalSymbol();
    char decimalSymbol =
        Utils.isEmpty(decimal) ? regional.getDecimalSeparator() : decimal.charAt(0);
    if (decimalSymbol != '.') {
      return CONVERT_LAZY_BYTES;
    }
    String grouping = format.getGroupingSymbol();
    char groupingSymbol =
        Utils.isEmpty(grouping) ? regional.getGroupingSeparator() : grouping.charAt(0);
    // A grouping symbol outside ASCII is caught by the ASCII check.
    return groupingSymbol < 0x80 ? groupingSymbol : NO_GROUPING_SYMBOL;
  }

  private static boolean isAsciiCompatible(String encoding) {
    try {
      return PGCopyRowBuffer.isAsciiCompatible(
          Utils.isEmpty(encoding) ? Charset.defaultCharset() : Charset.forName(encoding));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private void appendDate(
      PGCopyRowBuffer buf, StringBuilder text, int i, IValueMeta valueMeta, Object valueData)
      throws HopException {
    FastValueFormatter fast = data.dateFormatters[i];
    String typeName = valueMeta.getType() == IValueMeta.TYPE_DATE ? "date" : "timestamp";
    // Format the date in the right format.
    //
    switch (data.dateFormatChoices[i]) {
        // Pass the data along in the format chosen by the user OR in binary format...
        //
      case PGBulkLoaderMeta.NR_DATE_MASK_PASS_THROUGH:
        if (valueMeta.isStorageBinaryString()) {
          buf.append((byte[]) valueData);
        } else {
          text.setLength(0);
          if (fast != null && fast.append(text, valueData)) {
            buf.append(text);
          } else {
            String dateString = valueMeta.getString(valueData);
            if (dateString != null) {
              buf.append(dateString);
            }
          }
        }
        break;

        // Convert to a "YYYY-MM-DD" format
        //
      case PGBulkLoaderMeta.NR_DATE_MASK_DATE:
        appendConverted(buf, text, fast, data.dateMeta, valueMeta.getDate(valueData));
        break;

        // Convert to a "YYYY-MM-DD HH:MM:SS.mmm" format
        //
      case PGBulkLoaderMeta.NR_DATE_MASK_DATETIME:
        appendConverted(buf, text, fast, data.dateTimeMeta, valueMeta.getDate(valueData));
        break;

      default:
        throw new HopException(
            "PGBulkLoader doesn't know how to handle "
                + typeName
                + " (neither passthrough, nor date or datetime for field "
                + valueMeta.getName());
    }
  }

  private static void appendConverted(
      PGCopyRowBuffer buf,
      StringBuilder text,
      FastValueFormatter fast,
      IValueMeta dateMeta,
      Date date)
      throws HopValueException {
    text.setLength(0);
    if (date != null && fast != null && fast.append(text, date)) {
      buf.append(text);
      return;
    }
    String dateString = dateMeta.getString(date);
    if (dateString != null) {
      buf.append(dateString);
    }
  }

  /**
   * The end-of-input branch of {@link #processRow()} finishes the COPY and closes the connection. A
   * stop or an error in the middle of the stream never reaches it, leaving the COPY and its
   * connection open on the server - and with them the locks the load holds. Abort the copy here
   * ({@code endCopy()} would commit the partial rows, {@code cancelCopy()} discards them) and
   * release the connection. See <a href="https://github.com/apache/hop/issues/8288">issue 8288</a>.
   */
  @Override
  public void dispose() {
    try {
      if (pgCopyOut != null && pgCopyOut.isActive()) {
        pgCopyOut.cancelCopy();
      }
    } catch (SQLException e) {
      logError("Error cancelling the COPY command while stopping the transform", e);
    } finally {
      try {
        // Only close a copy that is no longer active. A still-active copy here means cancelCopy()
        // above threw (a broken connection, the likely case), and pgjdbc's close() runs endCopy()
        // on an active copy - which would commit the very rows we are trying to discard. The
        // disconnect() below tears the connection down regardless.
        if (pgCopyOut != null && !pgCopyOut.isActive()) {
          pgCopyOut.close();
        }
      } catch (IOException e) {
        logError("Error closing the COPY output stream", e);
      }
      pgCopyOut = null;
      if (data.db != null) {
        data.db.disconnect();
        data.db = null;
      }
    }
    super.dispose();
  }

  protected void verifyDatabaseConnection() throws HopException {
    // Confirming Database Connection is defined.
    if (meta.getConnection() == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "PGBulkLoaderMeta.GetSQL.NoConnectionDefined"));
    }
  }

  @Override
  public boolean init() {

    String enclosure = resolve(meta.getEnclosure());
    String separator = resolve(meta.getDelimiter());

    if (super.init()) {

      // Confirming Database Connection is defined.
      try {
        verifyDatabaseConnection();
      } catch (HopException ex) {
        logError(ex.getMessage());
        return false;
      }

      if (enclosure != null) {
        data.quote = enclosure.getBytes(COPY_ENCODING);
      } else {
        data.quote = new byte[] {};
      }
      if (separator != null) {
        data.separator = separator.getBytes(COPY_ENCODING);
      } else {
        data.separator = new byte[] {};
      }
      data.newline = Const.CR.getBytes(COPY_ENCODING);

      data.dateFormatChoices = new int[meta.getMappings().size()];
      for (int i = 0; i < data.dateFormatChoices.length; i++) {
        if (Utils.isEmpty(meta.getMappings().get(i).getDateMask())) {
          data.dateFormatChoices[i] = PGBulkLoaderMeta.NR_DATE_MASK_PASS_THROUGH;
        } else if (meta.getMappings()
            .get(i)
            .getDateMask()
            .equalsIgnoreCase(PGBulkLoaderMeta.DATE_MASK_DATE)) {
          data.dateFormatChoices[i] = PGBulkLoaderMeta.NR_DATE_MASK_DATE;
        } else if (meta.getMappings()
            .get(i)
            .getDateMask()
            .equalsIgnoreCase(PGBulkLoaderMeta.DATE_MASK_DATETIME)) {
          data.dateFormatChoices[i] = PGBulkLoaderMeta.NR_DATE_MASK_DATETIME;
        } else { // The default : just pass it along...
          data.dateFormatChoices[i] = PGBulkLoaderMeta.NR_DATE_MASK_PASS_THROUGH;
        }
      }
      return true;
    }
    return false;
  }
}
