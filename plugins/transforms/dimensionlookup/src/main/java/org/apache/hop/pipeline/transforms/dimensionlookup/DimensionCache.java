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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;

/**
 * This class will act as a special purpose dimension Cache. The idea here is to not only cache the
 * last version of a dimension entry, but all versions. So basically, the entry key is the natural
 * key as well as the from-to date range.
 *
 * <p>The way to achieve that result is to keep a sorted list in memory. Because we want as few
 * conversion errors as possible, we'll use the same row as we get from the database.
 */
public class DimensionCache implements Comparator<Object[]> {
  private static final Class<?> PKG = DimensionLookupMeta.class;
  private static final int MAX_VALIDATION_ERRORS = 3;

  private IRowMeta rowMeta;
  private List<Object[]> rowCache;
  private int[] keyIndexes;
  private int fromDateIndex;
  private int toDateIndex;

  /**
   * Create a new dimension cache object
   *
   * @param rowMeta the description of the rows to store
   * @param keyIndexes the indexes of the natural key (in that order)
   * @param fromDateIndex the field index where the start of the date range can be found
   * @param toDateIndex the field index where the end of the date range can be found
   */
  public DimensionCache(IRowMeta rowMeta, int[] keyIndexes, int fromDateIndex, int toDateIndex) {
    this.rowMeta = rowMeta;
    this.keyIndexes = keyIndexes;
    this.fromDateIndex = fromDateIndex;
    this.toDateIndex = toDateIndex;
  }

  /**
   * Returns true when both dates are non-null and equal, representing a zero-length [from, to)
   * validity interval.
   */
  static boolean isZeroLengthValidity(Date fromDate, Date toDate) {
    return fromDate != null
        && toDate != null
        && !fromDate.before(toDate)
        && !toDate.before(fromDate);
  }

  /**
   * Returns a new list that excludes rows with zero-length validity intervals.
   *
   * @param rowMeta the row layout of the cache rows
   * @param rows the rows loaded from the dimension table
   * @param fromDateIndex the index of the date-from field
   * @param toDateIndex the index of the date-to field
   * @return the filtered rows
   */
  public static List<Object[]> excludeZeroLengthValidityRows(
      IRowMeta rowMeta, List<Object[]> rows, int fromDateIndex, int toDateIndex) {
    List<Object[]> filteredRows = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      try {
        Date fromDate = rowMeta.getValueMeta(fromDateIndex).getDate(row[fromDateIndex]);
        Date toDate = rowMeta.getValueMeta(toDateIndex).getDate(row[toDateIndex]);
        if (!isZeroLengthValidity(fromDate, toDate)) {
          filteredRows.add(row);
        }
      } catch (Exception e) {
        throw new HopRuntimeException(e);
      }
    }
    return filteredRows;
  }

  /**
   * Add a row to the back of the list
   *
   * @param row the row to add
   */
  public void addRow(Object[] row) {
    rowCache.add(row);
  }

  /**
   * Get a row from the cache on a certain index
   *
   * @param index the index to look for
   * @return the row on the specified index
   */
  public Object[] getRow(int index) {
    return rowCache.get(index);
  }

  /**
   * Insert a row into the list on a certain index
   *
   * @param index the index on which the row should be inserted
   * @param row the row to add
   */
  public void addRow(int index, Object[] row) {
    rowCache.add(index, row);
  }

  /**
   * Looks up a row in the (sorted) cache.
   *
   * @param lookupRowData The data of the lookup row. Make sure that on the index of the from date,
   *     you put the lookup date.
   * @throws org.apache.hop.core.exception.HopException in case there are conversion errors during
   *     the lookup of the row
   */
  public int lookupRow(Object[] lookupRowData) throws HopException {
    try {
      // First perform the lookup!
      //
      int index = Collections.binarySearch(rowCache, lookupRowData, this);
      if (index < 0) {
        // What we have now is the insertion point.
        // Since we only compare on the start of the date range (see also: below in
        // Compare.compare())
        // we will usually get the insertion point of the row
        // However, that insertion point is the actual row index IF the supplied lookup date (in the
        // lookup row) is
        // between
        //
        // This row at the insertion point where the natural keys match and the start
        //
        int insertionPoint = -(index + 1);
        if (insertionPoint < rowCache.size() - 1) {
          // Get the row in question
          //
          Object[] row = rowCache.get(insertionPoint);

          // See if the natural key matches...
          //
          int cmp = rowMeta.compare(row, lookupRowData, keyIndexes);
          if (cmp == 0) {
            // The natural keys match, now see if the lookup date (lookupRowData[fromDateIndex]) is
            // between
            // row[fromDateIndex] and row[toDateIndex]
            //
            Date fromDate = rowMeta.getDate(row, fromDateIndex);
            Date toDate = rowMeta.getDate(row, toDateIndex);
            Date lookupDate = rowMeta.getDate(lookupRowData, fromDateIndex);

            if (fromDate == null && toDate != null) {
              // This is the case where the fromDate is null and the toDate is not.
              // This is a special case where null as a start date means -Infinity
              //
              if (toDate.compareTo(lookupDate) > 0) {
                return insertionPoint; // found the key!!
              } else {
                // This should never happen, it's a flaw in the data or the binary search
                // algorithm...
                // TODO: print the row perhaps?
                //
                throw new HopException(
                    "Key sorting problem detected during row cache lookup: the lookup date of "
                        + "the row retrieved is higher than or equal to the end of the date range.");
              }
            } else if (fromDate != null && toDate == null) {
              // This is the case where the toDate is null and the fromDate is not.
              // This is a special case where null as an end date means +Infinity
              //
              if (fromDate.compareTo(lookupDate) <= 0) {
                return insertionPoint; // found the key!!
              } else {
                // This should never happen, it's a flaw in the data or the binary search
                // algorithm...
                // TODO: print the row perhaps?
                //
                throw new HopException(
                    "Key sorting problem detected during row cache lookup: the lookup date of the row "
                        + "retrieved is lower than or equal to the start of the date range.");
              }
            } else {
              // Both dates are available: simply see if the lookup date falls in between...
              //
              if (fromDate.compareTo(lookupDate) <= 0 && toDate.compareTo(lookupDate) > 0) {
                return insertionPoint;
              }
              // Else this is a cache miss.
            }
          }
        }
      }
      return index;
    } catch (HopRuntimeException e) {
      throw new HopException(e);
    }
  }

  /**
   * Validate dimension rows before sorting the cache. Overlapping or invalid date ranges for the
   * same natural key cause the lookup comparator to violate its contract during {@link
   * #sortRows()}.
   *
   * @param schemaTable the quoted schema/table combination for error reporting
   * @throws HopTransformException when the cache data cannot be sorted safely
   */
  public void validateRowsForSort(String schemaTable) throws HopTransformException {
    try {
      if (rowCache == null || rowCache.isEmpty()) {
        return;
      }

      List<String> errors = new ArrayList<>();

      for (Object[] row : rowCache) {
        if (errors.size() >= MAX_VALIDATION_ERRORS) {
          break;
        }
        Date fromDate = getDateFromRow(row, fromDateIndex);
        Date toDate = getDateFromRow(row, toDateIndex);
        if (isZeroLengthValidity(fromDate, toDate)
            || (fromDate != null && toDate != null && fromDate.after(toDate))) {
          errors.add(
              BaseMessages.getString(
                  PKG,
                  "DimensionLookup.Exception.CacheInvalidDateRange",
                  formatNaturalKey(row),
                  formatDateRange(fromDate, toDate)));
        }
      }

      List<Object[]> sorted = new ArrayList<>(rowCache);
      sorted.sort(
          (o1, o2) -> {
            int cmp = compareNaturalKeys(o1, o2);
            if (cmp != 0) {
              return cmp;
            }
            return compareFromDates(o1, o2);
          });

      for (int i = 1; i < sorted.size() && errors.size() < MAX_VALIDATION_ERRORS; i++) {
        Object[] previousRow = sorted.get(i - 1);
        Object[] currentRow = sorted.get(i);
        if (compareNaturalKeys(previousRow, currentRow) != 0) {
          continue;
        }

        Date previousFrom = getDateFromRow(previousRow, fromDateIndex);
        Date previousTo = getDateFromRow(previousRow, toDateIndex);
        Date currentFrom = getDateFromRow(currentRow, fromDateIndex);
        Date currentTo = getDateFromRow(currentRow, toDateIndex);
        if (rangesOverlap(previousFrom, previousTo, currentFrom, currentTo)) {
          errors.add(
              BaseMessages.getString(
                  PKG,
                  "DimensionLookup.Exception.CacheOverlappingDateRanges",
                  formatNaturalKey(previousRow),
                  formatDateRange(previousFrom, previousTo),
                  formatDateRange(currentFrom, currentTo)));
        }
      }

      if (!errors.isEmpty()) {
        StringBuilder message = new StringBuilder();
        message.append(
            BaseMessages.getString(
                PKG, "DimensionLookup.Exception.CacheSortValidationFailed", schemaTable));
        for (String error : errors) {
          message.append(Const.CR).append(error);
        }
        if (errors.size() >= MAX_VALIDATION_ERRORS) {
          message.append(Const.CR).append("...");
        }
        throw new HopTransformException(message.toString());
      }
    } catch (HopTransformException e) {
      throw e;
    } catch (HopRuntimeException e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "DimensionLookup.Exception.CacheSortValidationFailed", schemaTable),
          e);
    }
  }

  public void sortRows() {
    Collections.sort(rowCache, this);
  }

  private int compareNaturalKeys(Object[] row1, Object[] row2) {
    try {
      return rowMeta.compare(row1, row2, keyIndexes);
    } catch (Exception e) {
      throw new HopRuntimeException(e);
    }
  }

  private Date getDateFromRow(Object[] row, int dateIndex) {
    try {
      return rowMeta.getValueMeta(dateIndex).getDate(row[dateIndex]);
    } catch (Exception e) {
      throw new HopRuntimeException(e);
    }
  }

  private int compareFromDates(Object[] row1, Object[] row2) {
    Date from1 = getDateFromRow(row1, fromDateIndex);
    Date from2 = getDateFromRow(row2, fromDateIndex);
    if (from1 == null && from2 == null) {
      return 0;
    }
    if (from1 == null) {
      return -1;
    }
    if (from2 == null) {
      return 1;
    }
    return from1.compareTo(from2);
  }

  /**
   * Returns true when the half-open intervals [from1, to1) and [from2, to2) overlap. A null from
   * date means -infinity and a null to date means +infinity.
   */
  private boolean rangesOverlap(Date from1, Date to1, Date from2, Date to2) {
    boolean from1BeforeTo2 = (to2 == null) || (from1 == null) || from1.before(to2);
    boolean from2BeforeTo1 = (to1 == null) || (from2 == null) || from2.before(to1);
    return from1BeforeTo2 && from2BeforeTo1;
  }

  private String formatNaturalKey(Object[] row) {
    try {
      if (keyIndexes == null || keyIndexes.length == 0) {
        return "(no natural key)";
      }

      StringBuilder naturalKey = new StringBuilder();
      for (int i = 0; i < keyIndexes.length; i++) {
        if (i > 0) {
          naturalKey.append(", ");
        }
        naturalKey.append(rowMeta.getString(row, keyIndexes[i]));
      }
      return naturalKey.toString();
    } catch (Exception e) {
      throw new HopRuntimeException(e);
    }
  }

  private String formatDateRange(Date fromDate, Date toDate) {
    try {
      String fromDateText = "-infinity";
      String toDateText = "+infinity";
      if (fromDate != null) {
        fromDateText = rowMeta.getValueMeta(fromDateIndex).getString(fromDate);
      }
      if (toDate != null) {
        toDateText = rowMeta.getValueMeta(toDateIndex).getString(toDate);
      }
      return "[" + fromDateText + ", " + toDateText + ")";
    } catch (Exception e) {
      throw new HopRuntimeException(e);
    }
  }

  /**
   * Compare 2 rows of data using the natural keys and indexes specified.
   *
   * @param o1
   * @param o2
   * @return
   */
  @Override
  public int compare(Object[] o1, Object[] o2) {
    try {
      // First compare on the natural keys...
      //
      int cmp = rowMeta.compare(o1, o2, keyIndexes);
      if (cmp != 0) {
        return cmp;
      }

      // Then see if the start of the date range of o2 falls between the start and end of o2
      //
      IValueMeta fromDateMeta = rowMeta.getValueMeta(fromDateIndex);
      IValueMeta toDateMeta = rowMeta.getValueMeta(toDateIndex);

      Date fromDate = fromDateMeta.getDate(o1[fromDateIndex]);
      Date toDate = toDateMeta.getDate(o1[toDateIndex]);
      Date lookupDate = fromDateMeta.getDate(o2[fromDateIndex]);

      int fromCmpLookup = 0;
      if (fromDate == null) {
        if (lookupDate == null) {
          fromCmpLookup = 0;
        } else {
          fromCmpLookup = -1;
        }
      } else {
        if (lookupDate == null) {
          fromCmpLookup = 1;
        } else {
          fromCmpLookup = fromDateMeta.compare(fromDate, lookupDate);
        }
      }
      if (fromCmpLookup < 0 && toDate != null) {
        int toCmpLookup = toDateMeta.compare(toDate, lookupDate);
        if (toCmpLookup > 0) {
          return 0;
        }
      }
      return fromCmpLookup;
    } catch (Exception e) {
      throw new HopRuntimeException(e);
    }
  }

  /**
   * @return the rowMeta
   */
  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  /**
   * @param rowMeta the rowMeta to set
   */
  public void setRowMeta(IRowMeta rowMeta) {
    this.rowMeta = rowMeta;
  }

  /**
   * @return the rowCache
   */
  public List<Object[]> getRowCache() {
    return rowCache;
  }

  /**
   * @param rowCache the rowCache to set
   */
  public void setRowCache(List<Object[]> rowCache) {
    this.rowCache = rowCache;
  }

  /**
   * @return the keyIndexes
   */
  public int[] getKeyIndexes() {
    return keyIndexes;
  }

  /**
   * @param keyIndexes the keyIndexes to set
   */
  public void setKeyIndexes(int[] keyIndexes) {
    this.keyIndexes = keyIndexes;
  }

  /**
   * @return the fromDateIndex
   */
  public int getFromDateIndex() {
    return fromDateIndex;
  }

  /**
   * @param fromDateIndex the fromDateIndex to set
   */
  public void setFromDateIndex(int fromDateIndex) {
    this.fromDateIndex = fromDateIndex;
  }

  /**
   * @return the toDateIndex
   */
  public int getToDateIndex() {
    return toDateIndex;
  }

  /**
   * @param toDateIndex the toDateIndex to set
   */
  public void setToDateIndex(int toDateIndex) {
    this.toDateIndex = toDateIndex;
  }
}
