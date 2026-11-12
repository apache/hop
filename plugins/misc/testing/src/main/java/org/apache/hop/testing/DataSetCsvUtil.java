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

package org.apache.hop.testing;

import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * The implementation of a CSV Data Set Group We simply write/read the rows without a header into a
 * file defined by the tableName in the data set
 */
public class DataSetCsvUtil {
  /**
   * Storage mask for Number and BigNumber fields without a format. DecimalFormat prints the
   * shortest decimal representation of a double, so 3.14 is written as "3.14" while no digits are
   * ever dropped (340 is the maximum number of fraction digits DecimalFormat honours for a double).
   * Without it a BigNumber falls back to the default mask, which keeps 19 fraction digits, or to a
   * pattern built from its length and precision, which rounds and zero-pads. Declared precision is
   * applied when comparing against golden data, not when storing.
   */
  static final String NUMBER_STORAGE_MASK = "0." + "#".repeat(340);

  public static void setValueFormats(IRowMeta rowMeta) {
    for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
      if (StringUtils.isEmpty(valueMeta.getConversionMask())) {
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_INTEGER:
            valueMeta.setConversionMask("0");
            break;
          case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER:
            valueMeta.setConversionMask(NUMBER_STORAGE_MASK);
            break;
          case IValueMeta.TYPE_DATE:
            valueMeta.setConversionMask("yyyyMMdd-HHmmss.SSS");
            break;
          default:
            break;
        }
      }

      // Force decimal separator for non English system (HOP-4190).
      if (valueMeta.isNumber() || valueMeta.isBigNumber()) {
        valueMeta.setDecimalSymbol(".");
      }
    }
  }

  /** Receives the rows of a data set one at a time, in file order. */
  @FunctionalInterface
  public interface IRowHandler {
    /**
     * @param row the next row, in the layout of {@link DataSet#getSetRowMeta()}
     * @return true to go on reading, false to stop
     */
    boolean handleRow(Object[] row) throws HopException;
  }

  public static final List<Object[]> getAllRows(IVariables variables, DataSet dataSet)
      throws HopException {
    List<Object[]> rows = new ArrayList<>();
    readRows(variables, dataSet, rows::add);
    return rows;
  }

  /**
   * Reads the rows of a data set and hands each one to the handler as soon as it is parsed, so a
   * large data set never has to fit in memory. A data set without a file has no rows yet: the
   * handler is not called.
   *
   * @param handler receives each row; return false to stop reading
   */
  public static final void readRows(IVariables variables, DataSet dataSet, IRowHandler handler)
      throws HopException {
    IRowMeta setRowMeta = dataSet.getSetRowMeta();
    setValueFormats(setRowMeta);
    String dataSetFilename = dataSet.getActualDataSetFilename(variables);

    // One String meta per field, prepared once: it carries the field's mask and symbols for the
    // conversion. The field metadata keeps its number and date formats from row to row.
    int nrFields = setRowMeta.size();
    IValueMeta[] valueMetas = new IValueMeta[nrFields];
    IValueMeta[] stringMetas = new IValueMeta[nrFields];
    for (int i = 0; i < nrFields; i++) {
      valueMetas[i] = setRowMeta.getValueMeta(i);
      stringMetas[i] = new ValueMetaString("constant");
      stringMetas[i].setConversionMetadata(valueMetas[i]);
    }

    try {
      FileObject file = HopVfs.getFileObject(dataSetFilename);
      if (!file.exists()) {
        // This is fine.  We haven't put rows in yet.
        //
        return;
      }

      try (Reader reader =
              new InputStreamReader(new BufferedInputStream(HopVfs.getInputStream(file)));
          CSVParser csvParser = new CSVParser(reader, getCsvFormat(setRowMeta)); ) {
        for (CSVRecord csvRecord : csvParser) {
          if (csvRecord.getRecordNumber() > 1) {
            Object[] row = RowDataUtil.allocateRowData(nrFields);
            for (int i = 0; i < nrFields; i++) {
              if (i < csvRecord.size()) {
                row[i] = csvValueToField(valueMetas[i], stringMetas[i], csvRecord.get(i));
              }
            }
            if (!handler.handleRow(row)) {
              return;
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Unable to get all rows for CSV data set '" + dataSet.getName() + "'", e);
    }
  }

  /**
   * Get the rows for this data set in the format of the data set.
   *
   * @param variables
   * @param log the logging channel to which you can write.
   * @param location The fields to obtain in the order given
   * @return The rows for the given location
   * @throws HopException
   */
  public static final List<Object[]> getAllRows(
      IVariables variables, ILogChannel log, DataSet dataSet, PipelineUnitTestSetLocation location)
      throws HopException {

    IRowMeta setRowMeta = dataSet.getSetRowMeta();

    // The row description of the output of this transform...
    //
    final IRowMeta outputRowMeta = dataSet.getMappedDataSetFieldsRowMeta(location);

    setValueFormats(setRowMeta);
    String dataSetFilename = dataSet.getActualDataSetFilename(variables);
    List<Object[]> rows = new ArrayList<>();
    final ValueMetaString constantValueMeta = new ValueMetaString("constant");

    try {

      FileObject file = HopVfs.getFileObject(dataSetFilename);
      if (!file.exists()) {
        throw new HopException("File " + dataSetFilename + " doesn't exist!");
      }

      List<String> sortFields = location.getFieldOrder();

      // See how we mapped the fields
      //
      List<PipelineUnitTestFieldMapping> fieldMappings = location.getFieldMappings();
      int[] dataSetFieldIndexes = new int[fieldMappings.size()];
      for (int i = 0; i < fieldMappings.size(); i++) {
        PipelineUnitTestFieldMapping fieldMapping = fieldMappings.get(i);
        String dataSetFieldName = fieldMapping.getDataSetFieldName();
        dataSetFieldIndexes[i] = setRowMeta.indexOfValue(dataSetFieldName);
      }

      try (Reader reader =
              new InputStreamReader(new BufferedInputStream(HopVfs.getInputStream(file)));
          CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT); ) {
        for (CSVRecord csvRecord : csvParser) {
          if (csvRecord.getRecordNumber() > 1) {
            Object[] row = RowDataUtil.allocateRowData(dataSetFieldIndexes.length);

            // Only get certain values...
            //
            for (int i = 0; i < dataSetFieldIndexes.length; i++) {
              int index = dataSetFieldIndexes[i];

              if (index >= 0 && index < setRowMeta.size() && index < csvRecord.size()) {
                IValueMeta valueMeta = setRowMeta.getValueMeta(index);
                constantValueMeta.setConversionMetadata(valueMeta);
                String value = csvRecord.get(index);
                row[i] = csvValueToField(valueMeta, constantValueMeta, value);
              } else {
                row[i] = null;
              }
            }
            rows.add(row);
          }
        }
      }

      // Which fields are we sorting on (if any)
      //
      int[] sortIndexes = new int[sortFields.size()];
      for (int i = 0; i < sortIndexes.length; i++) {
        sortIndexes[i] = outputRowMeta.indexOfValue(sortFields.get(i));
      }

      if (outputRowMeta.isEmpty()) {
        log.logError(
            "WARNING: No field mappings selected for data set '"
                + dataSet.getName()
                + "', returning empty set of rows");
        return new ArrayList<>();
      }

      if (!sortFields.isEmpty() && StringUtils.isNotEmpty(sortFields.getFirst())) {
        // Sort the rows...
        //
        rows.sort(
            (o1, o2) -> {
              try {
                return outputRowMeta.compare(o1, o2, sortIndexes);
              } catch (HopValueException e) {
                throw new HopRuntimeException("Unable to compare 2 rows", e);
              }
            });
      }

      return rows;

    } catch (Exception e) {
      throw new HopException(
          "Unable to get all rows for database data set '" + dataSet.getName() + "'", e);
    }
  }

  public static final void writeDataSetData(
      IVariables variables, DataSet dataSet, IRowMeta rowMeta, List<Object[]> rows)
      throws HopException {
    try (DataSetCsvWriter writer = new DataSetCsvWriter(variables, dataSet, rowMeta)) {
      for (Object[] row : rows) {
        writer.writeRow(row);
      }
    }
  }

  /**
   * Binary data-set fields are stored as lowercase hex (no {@code \\x} prefix). Everything else
   * uses the field's normal string conversion.
   */
  static Object csvValueToField(IValueMeta valueMeta, IValueMeta stringMeta, String value)
      throws HopException {
    if (valueMeta.isBinary()) {
      return decodeHex(value, valueMeta.getName());
    }
    if (value != null && (valueMeta.isNumber() || valueMeta.isBigNumber())) {
      // Earlier versions padded a BigNumber with a length to " 00001234.57"
      value = value.trim();
    }
    return valueMeta.convertData(stringMeta, value);
  }

  static byte[] decodeHex(String value, String fieldName) throws HopException {
    if (value == null || value.isEmpty()) {
      return null;
    }
    try {
      return Hex.decodeHex(value);
    } catch (DecoderException e) {
      throw new HopException(
          "Unable to decode hex binary value for field '" + fieldName + "': " + value, e);
    }
  }

  public static CSVFormat getCsvFormat(IRowMeta rowMeta) {
    return CSVFormat.DEFAULT
        .withHeader(rowMeta.getFieldNames())
        .withQuote('\"')
        .withQuoteMode(QuoteMode.MINIMAL);
  }
}
