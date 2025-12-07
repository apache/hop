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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DimensionUpdateType.INSERT;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DimensionUpdateType.PUNCH_THROUGH;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.StartDateAlternative.COLUMN_VALUE;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.StartDateAlternative.NONE;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.StartDateAlternative.NULL;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.StartDateAlternative.SYSTEM_DATE;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.hash.ByteArrayHashMap;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLField;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLFields;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLKey;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DimensionUpdateType;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.TechnicalKeyCreationMethod;

/** Manages a slowly changing dimension (lookup or update) */
public class DimensionLookup extends BaseTransform<DimensionLookupMeta, DimensionLookupData> {

  private static final Class<?> PKG = DimensionLookupMeta.class;
  public static final String CONST_DIMENSION_LOOKUP_EXCEPTION_KEY_FIELD_NOT_FOUND =
      "DimensionLookup.Exception.KeyFieldNotFound";
  public static final String CONST_UPDATE = "UPDATE ";
  public static final String CONST_AND = "AND   ";
  public static final String CONST_WHERE = " WHERE ";

  int[] columnLookupArray = null;

  public DimensionLookup(
      TransformMeta transformMeta,
      DimensionLookupMeta meta,
      DimensionLookupData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...

      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    if (first) {
      first = false;

      data.schemaTable =
          data.databaseMeta.getQuotedSchemaTableCombination(
              this, data.realSchemaName, data.realTableName);

      data.inputRowMeta = getInputRowMeta().clone();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Get the fields that need conversion to normal storage...
      // Modify the storage type of the input data...
      //
      data.lazyList = new ArrayList<>();
      for (int i = 0; i < data.inputRowMeta.size(); i++) {
        IValueMeta valueMeta = data.inputRowMeta.getValueMeta(i);
        if (valueMeta.isStorageBinaryString()) {
          data.lazyList.add(i);
          valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
        }
      }

      // The start date value column (if applicable)
      //
      data.startDateFieldIndex = -1;
      if (data.startDateAlternative == COLUMN_VALUE) {
        data.startDateFieldIndex = data.inputRowMeta.indexOfValue(meta.getStartDateFieldName());
        if (data.startDateFieldIndex < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG,
                  "DimensionLookup.Exception.StartDateValueColumnNotFound",
                  meta.getStartDateFieldName()));
        }
      }
      DLFields f = meta.getFields();

      // Lookup values
      data.keynrs = new int[f.getKeys().size()];
      for (int i = 0; i < data.keynrs.length; i++) {
        DLKey key = f.getKeys().get(i);

        data.keynrs[i] = data.inputRowMeta.indexOfValue(key.getName());
        if (data.keynrs[i] < 0) { // couldn't find field!
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, CONST_DIMENSION_LOOKUP_EXCEPTION_KEY_FIELD_NOT_FOUND, key.getName()));
        }
      }

      // Return values
      data.fieldnrs = new int[f.getFields().size()];
      for (int i = 0; i < data.fieldnrs.length; i++) {
        DLField field = f.getFields().get(i);
        if (isLookupOrUpdateTypeWithArgument(meta.isUpdate(), field)) {
          data.fieldnrs[i] = data.outputRowMeta.indexOfValue(field.getName());
          if (data.fieldnrs[i] < 0) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, CONST_DIMENSION_LOOKUP_EXCEPTION_KEY_FIELD_NOT_FOUND, field.getName()));
          }
        } else {
          data.fieldnrs[i] = -1;
        }
      }

      if (!meta.isUpdate() && meta.isPreloadingCache()) {
        preloadCache();
      } else {
        // Caching...
        //
        if (data.cacheKeyRowMeta == null) {
          // KEY : the natural key(s)
          //
          data.cacheKeyRowMeta = new RowMeta();
          for (int i = 0; i < data.keynrs.length; i++) {
            IValueMeta key = data.inputRowMeta.getValueMeta(data.keynrs[i]);
            data.cacheKeyRowMeta.addValueMeta(key.clone());
          }

          data.cache =
              new ByteArrayHashMap(
                  meta.getCacheSize() > 0 ? meta.getCacheSize() : 5000, data.cacheKeyRowMeta);
        }
      }

      if (StringUtils.isNotEmpty(f.getDate().getName())) {
        data.datefieldnr = data.inputRowMeta.indexOfValue(f.getDate().getName());
      } else {
        data.datefieldnr = -1;
      }

      // Initialize the start date value in case we don't have one in the input rows
      //
      data.valueDateNow = determineDimensionUpdatedDate(r);

      data.notFoundTk = (long) data.databaseMeta.getNotFoundTK(isAutoIncrement());

      if (getCopy() == 0) {
        checkDimZero();
      }

      setDimLookup(data.outputRowMeta);
    }

    // convert row to normal storage...
    //
    for (int lazyFieldIndex : data.lazyList) {
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(lazyFieldIndex);
      r[lazyFieldIndex] = valueMeta.convertToNormalStorageType(r[lazyFieldIndex]);
    }

    try {
      Object[] outputRow =
          lookupValues(data.inputRowMeta, r); // add new values to the row in rowset[0].
      putRow(data.outputRowMeta, outputRow); // copy row to output rowset(s)

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "DimensionLookup.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(
                PKG, "DimensionLookup.Log.TransformCanNotContinueForErrors", e.getMessage()));
        logError(Const.getStackTracker(e));
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "ISU001");
      }
    }

    return true;
  }

  public boolean isLookupOrUpdateTypeWithArgument(final boolean update, final DLField field)
      throws HopTransformException {
    // Lookup
    if (!update) {
      return true;
    }

    // Update type field
    DimensionUpdateType updateType = field.getUpdateType();
    if (updateType == null) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "DimensionLookup.Exception.MissingUpdateTypeField ", field.getName()));
    }
    return updateType.isWithArgument();
  }

  private Date determineDimensionUpdatedDate(Object[] row) throws HopException {
    if (data.datefieldnr < 0) {
      return getPipeline().getExecutionStartDate(); // start of pipeline...
    } else {
      Date rtn = data.inputRowMeta.getDate(row, data.datefieldnr); // Date field in the input row
      if (rtn != null) {
        return rtn;
      } else {
        String inputRowMetaStringMeta;
        try {
          inputRowMetaStringMeta = data.inputRowMeta.toStringMeta();
        } catch (Exception ex) {
          inputRowMetaStringMeta = "No row input meta";
        }
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DimensionLookup.Exception.NullDimensionUpdatedDate", inputRowMetaStringMeta));
      }
    }
  }

  /**
   * Pre-load the cache by reading the whole dimension table from disk...
   *
   * @throws HopException in case there is a database or cache problem.
   */
  private void preloadCache() throws HopException {
    try {

      DLFields f = meta.getFields();

      // Retrieve: tk, version, from, to, natural keys, retrieval fields.
      // Store these rows in the cache.
      //
      StringBuilder sql = new StringBuilder();

      sql.append("SELECT ").append(data.databaseMeta.quoteField(f.getReturns().getKeyField()));

      // Add the natural key field in the table.
      //
      for (DLKey key : f.getKeys()) {
        sql.append(", ").append(data.databaseMeta.quoteField(key.getLookup()));
      }

      // Add the extra fields to retrieve.
      //
      for (DLField field : f.getFields()) {
        sql.append(", ").append(data.databaseMeta.quoteField(field.getLookup()));
      }

      // Add the date range fields
      //
      sql.append(", ").append(data.databaseMeta.quoteField(f.getDate().getFrom()));
      sql.append(", ").append(data.databaseMeta.quoteField(f.getDate().getTo()));

      sql.append(" FROM ").append(data.schemaTable);

      if (isDetailed()) {
        logDetailed(
            "Pre-loading cache by reading from database with: " + Const.CR + sql + Const.CR);
      }

      List<Object[]> rows = data.db.getRows(sql.toString(), -1);
      IRowMeta rowMeta = data.db.getReturnRowMeta();

      data.preloadKeyIndexes = new int[f.getKeys().size()];
      for (int i = 0; i < data.preloadKeyIndexes.length; i++) {
        DLKey key = f.getKeys().get(i);
        // The field in the table:
        data.preloadKeyIndexes[i] = rowMeta.indexOfValue(key.getLookup());
      }
      data.preloadFromDateIndex = rowMeta.indexOfValue(f.getDate().getFrom());
      data.preloadToDateIndex = rowMeta.indexOfValue(f.getDate().getTo());

      data.preloadCache =
          new DimensionCache(
              rowMeta, data.preloadKeyIndexes, data.preloadFromDateIndex, data.preloadToDateIndex);
      data.preloadCache.setRowCache(rows);

      logDetailed("Sorting the cache rows...");
      data.preloadCache.sortRows();
      logDetailed("Sorting of cached rows finished.");

      // Also see what indexes to take to populate the lookup row...
      // We only ever compare indexes and the lookup date in the cache, the rest is not needed...
      //
      data.preloadIndexes = new ArrayList<>();
      for (int i = 0; i < f.getKeys().size(); i++) {
        DLKey key = f.getKeys().get(i);
        int index = data.inputRowMeta.indexOfValue(key.getName());
        if (index < 0) {
          // Just to be safe...
          //
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, CONST_DIMENSION_LOOKUP_EXCEPTION_KEY_FIELD_NOT_FOUND, key.getName()));
        }
        data.preloadIndexes.add(index);
      }

      // This is all for now...
    } catch (Exception e) {
      throw new HopException("Error encountered during cache pre-load", e);
    }
  }

  private synchronized Object[] lookupValues(IRowMeta rowMeta, Object[] row) throws HopException {
    DLFields f = meta.getFields();

    // Determine the lookup date ("now") if we have a field that carries said
    // date.
    // If not, the system date is taken.
    //
    Date valueDate = determineDimensionUpdatedDate(row);

    IRowMeta lookupRowMeta;
    Object[] lookupRow;
    Object[] returnRow = null;
    if (!meta.isUpdate() && meta.isPreloadingCache()) {
      // Obtain a result row from the pre-load cache...
      //
      // Create a row to compare with
      //
      IRowMeta preloadRowMeta = data.preloadCache.getRowMeta();

      // In this case it's all the same. (simple)
      //
      data.returnRowMeta = data.preloadCache.getRowMeta();
      lookupRowMeta = preloadRowMeta;
      lookupRow = new Object[preloadRowMeta.size()];

      // Assemble the lookup row, convert data if needed...
      //
      for (int i = 0; i < data.preloadIndexes.size(); i++) {
        int from = data.preloadIndexes.get(i); // Input row index
        int to = data.preloadCache.getKeyIndexes()[i]; // Lookup row index

        // From data type...
        //
        IValueMeta fromValueMeta = rowMeta.getValueMeta(from);

        // to date type...
        //
        IValueMeta toValueMeta = data.preloadCache.getRowMeta().getValueMeta(to);

        // From value:
        //
        Object fromData = row[from];

        // To value:
        //
        Object toData = toValueMeta.convertData(fromValueMeta, fromData);

        // Set the key in the row...
        //
        lookupRow[to] = toData;
      }

      // Also set the lookup date on the "end of date range" (toDate) position
      //
      lookupRow[data.preloadFromDateIndex] = valueDate;

      // Look up the row in the pre-load cache...
      //
      int index = data.preloadCache.lookupRow(lookupRow);
      if (index >= 0) {
        returnRow = data.preloadCache.getRow(index);
      }
    } else {
      lookupRow = new Object[data.lookupRowMeta.size()];
      lookupRowMeta = data.lookupRowMeta;

      // Construct the lookup row...
      //
      int outputIndex = 0;
      for (int i = 0; i < f.getKeys().size(); i++) {
        lookupRow[outputIndex++] = row[data.keynrs[i]];
      }

      lookupRow[outputIndex++] = valueDate; // ? >= date_from
      lookupRow[outputIndex] = valueDate; // ? < date_to

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "DimensionLookup.Log.LookupRow")
                + data.lookupRowMeta.getString(lookupRow));
      }

      // Do the lookup and see if we can find anything in the database.
      // But before that, let's see if we can find anything in the cache
      //
      if (meta.getCacheSize() >= 0) {
        returnRow = getFromCache(lookupRow, valueDate);
      }

      // Nothing found in the cache?
      // Perform the lookup in the database...
      //
      if (returnRow == null) {
        data.db.setValues(data.lookupRowMeta, lookupRow, data.prepStatementLookup);
        returnRow = data.db.getLookup(data.prepStatementLookup);
        data.returnRowMeta = data.db.getReturnRowMeta();

        incrementLinesInput();

        if (returnRow != null && meta.getCacheSize() >= 0) {
          addToCache(lookupRow, returnRow);
        }
      }
    }

    // This next block of code handles the dimension key LOOKUP ONLY.
    // We handle this case where "update = false" first for performance reasons
    //
    if (!meta.isUpdate()) {
      if (returnRow == null) {
        returnRow = new Object[data.returnRowMeta.size()];
        returnRow[0] = data.notFoundTk;

        if (meta.getCacheSize() >= 0) { // need -oo to +oo as well...
          returnRow[returnRow.length - 2] = data.minDate;
          returnRow[returnRow.length - 1] = data.maxDate;
        }
      }
    } else {
      // This is the "update=true" case where we update the dimension table...
      // It is an "Insert - update" algorithm for slowly changing dimensions
      //
      // The dimension entry was not found, we need to add it!
      //
      Long valueVersion;
      Long technicalKey;
      Date valueDateFrom = null;
      Date valueDateTo = null;
      if (returnRow == null) {
        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "DimensionLookup.Log.NoDimensionEntryFound")
                  + lookupRowMeta.getString(lookupRow)
                  + ")");
        }

        // Date range: ]-oo,+oo[
        //

        if (data.startDateAlternative == SYSTEM_DATE) {
          // use the time the transform execution begins as the date from.
          // before, the current system time was used. this caused an exclusion of the row in the
          // lookup portion of the transform that uses this 'valueDate' and not the current time.
          // the result was multiple inserts for what should have been 1
          valueDateFrom = valueDate;
        } else {
          valueDateFrom = data.minDate;
        }

        valueDateTo = data.maxDate;
        valueVersion = 1L; // Versions always start at 1.

        // get a new value from the sequence generator chosen.
        //
        TechnicalKeyCreationMethod creationMethod = f.getReturns().getCreationMethod();
        if (creationMethod == null) {
          throw new HopTransformException(
              "Please specify a valid method for creating new technical (surrogate) keys");
        }
        switch (creationMethod) {
          case TABLE_MAXIMUM:
            // What's the next value for the technical key?
            technicalKey =
                data.db.getNextValue(
                    data.realSchemaName, data.realTableName, f.getReturns().getKeyField());
            break;
          case AUTO_INCREMENT:
            technicalKey = null; // Set to null to flag auto-increment usage
            break;
          case SEQUENCE:
            technicalKey =
                data.db.getNextSequenceValue(
                    data.realSchemaName, meta.getSequenceName(), f.getReturns().getKeyField());
            if (technicalKey != null && isRowLevel()) {
              logRowlevel(
                  BaseMessages.getString(PKG, "DimensionLookup.Log.FoundNextSequence")
                      + technicalKey.toString());
            }
            break;
          default:
            throw new HopTransformException(
                "Unknown technical key creation method encountered: " + creationMethod);
        }

        /*
         * INSERT INTO table(version, datefrom, dateto, fieldlookup) VALUES(valueVersion, valueDateFrom, valueDateTo,
         * row.fieldnrs)
         */
        technicalKey =
            dimInsert(
                data.inputRowMeta,
                row,
                technicalKey,
                true,
                valueVersion,
                valueDateFrom,
                valueDateTo);

        incrementLinesOutput();
        returnRow = new Object[data.returnRowMeta.size()];
        int returnIndex = 0;

        returnRow[returnIndex] = technicalKey;

        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "DimensionLookup.Log.AddedDimensionEntry")
                  + data.returnRowMeta.getString(returnRow));
        }
      } else {
        //
        // The entry was found: do we need to insert, update or both?
        //
        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "DimensionLookup.Log.DimensionEntryFound")
                  + data.returnRowMeta.getString(returnRow));
        }

        // What's the key? The first value of the return row
        technicalKey = data.returnRowMeta.getInteger(returnRow, 0);
        valueVersion = data.returnRowMeta.getInteger(returnRow, 1);

        // Date range: ]-oo,+oo[
        valueDateFrom = meta.getMinDate();
        valueDateTo = meta.getMaxDate();

        // The other values, we compare with
        int cmp;

        // If everything is the same: don't do anything
        // If one of the fields is different: insert or update
        // If all changed fields have update = Y, update
        // If one of the changed fields has update = N, insert

        boolean insert = false;
        boolean identical = true;
        boolean punch = false;

        // Column lookup array : initialize all to -1
        //
        if (columnLookupArray == null) {
          columnLookupArray = new int[f.getFields().size()];
          Arrays.fill(columnLookupArray, -1);
        }
        int returnRowColNum = -1;
        String findColumn = null;
        for (int i = 0; i < f.getFields().size(); i++) {
          DLField field = f.getFields().get(i);
          if (data.fieldnrs[i] >= 0) {
            // Only compare real fields, not last updated row, last version, etc
            //
            IValueMeta v1 = data.outputRowMeta.getValueMeta(data.fieldnrs[i]);
            Object valueData1 = row[data.fieldnrs[i]];
            findColumn = field.getName();
            // find the returnRowMeta based on the field in the fieldLookup list
            IValueMeta v2 = null;
            Object valueData2 = null;
            // See if it's already been computed.
            returnRowColNum = columnLookupArray[i];
            if (returnRowColNum == -1) {
              // It hasn't been found yet - search the list and make sure we're comparing
              // the right column to the right column.
              for (int j = 2;
                  j < data.returnRowMeta.size();
                  j++) { // starting at 2 because I know that 0 and 1 are
                // poked in by Hop.
                v2 = data.returnRowMeta.getValueMeta(j);
                if ((v2.getName() != null)
                    && (v2.getName().equalsIgnoreCase(findColumn))) { // is this the
                  // right column?
                  columnLookupArray[i] =
                      j; // yes - record the "j" into the columnLookupArray at [i] for the next time
                  // through the loop
                  valueData2 = returnRow[j]; // get the valueData2 for comparison
                  break; // get outta here.
                } else {
                  // Reset to null because otherwise, we'll get a false finding at the end.
                  // This could be optimized to use a temporary variable to avoid the repeated set
                  // if necessary
                  // but it will never be as slow as the database lookup anyway
                  v2 = null;
                }
              }
            } else {
              // We have a value in the columnLookupArray - use the value stored there.
              v2 = data.returnRowMeta.getValueMeta(returnRowColNum);
              valueData2 = returnRow[returnRowColNum];
            }
            if (v2 == null) {
              // If we made it here, then maybe someone tweaked the XML in the pipeline.
              // We're matching a stream column to a column that doesn't really exist. Throw an
              // exception.
              //
              throw new HopTransformException(
                  BaseMessages.getString(
                      PKG,
                      "DimensionLookup.Exception.ErrorDetectedInComparingFields",
                      field.getName()));
            }

            try {
              cmp = v1.compare(valueData1, v2, valueData2);
            } catch (ClassCastException e) {
              throw new HopTransformException("Error comparing values", e);
            }

            // Not the same and update = 'N' --> insert
            if (cmp != 0) {
              identical = false;
            }

            // Field flagged for insert: insert
            if ((cmp != 0) && (field.getUpdateType() == INSERT)) {
              insert = true;
            }

            // Field flagged for punch-through (update all versions)
            if (cmp != 0 && field.getUpdateType() == PUNCH_THROUGH) {
              punch = true;
            }

            if (isRowLevel()) {
              logRowlevel(
                  BaseMessages.getString(
                      PKG,
                      "DimensionLookup.Log.ComparingValues",
                      "" + v1,
                      "" + v2,
                      String.valueOf(cmp),
                      String.valueOf(identical),
                      String.valueOf(insert),
                      String.valueOf(punch)));
            }
          }
        }

        // After comparing the record in the database and the data in the input
        // and taking into account the rules of the slowly changing dimension,
        // we found out if we need to perform an insert or an update.
        //
        if (!insert) {
          // We simply perform an update of row at key = valueKey.
          //
          if (!identical) {
            if (isRowLevel()) {
              logRowlevel(
                  BaseMessages.getString(PKG, "DimensionLookup.Log.UpdateRowWithValues")
                      + data.inputRowMeta.getString(row));
            }
            /*
             * UPDATE d_customer SET fieldlookup[] = row.getValue(fieldnrs) WHERE returnkey = dimkey
             */
            dimUpdate(rowMeta, row, technicalKey, valueDate);
            incrementLinesUpdated();

            // We need to capture this change in the cache as well...
            if (meta.getCacheSize() >= 0) {
              Object[] values =
                  getCacheValues(
                      rowMeta, row, technicalKey, valueVersion, valueDateFrom, valueDateTo);
              addToCache(lookupRow, values);
            }
          } else {
            if (isRowLevel()) {
              logRowlevel(BaseMessages.getString(PKG, "DimensionLookup.Log.SkipLine"));
            }
            // Don't do anything, everything is file in de dimension.
            incrementLinesSkipped();
          }
        } else {
          if (isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(PKG, "DimensionLookup.Log.InsertNewVersion")
                    + technicalKey.toString());
          }

          Long valueNewVersion = valueVersion + 1;
          // From date (valueDate) is calculated at the start of this method to
          // be either the system date or the value in a column
          //
          valueDateFrom = valueDate;
          valueDateTo = data.maxDate;

          // First try to use an AUTOINCREMENT field
          if (data.databaseMeta.supportsAutoinc() && isAutoIncrement()) {
            technicalKey = null; // value to accept new key...
          } else if (data.databaseMeta.supportsSequences()
              // Try to get the value by looking at a SEQUENCE (oracle mostly)
              && meta.getSequenceName() != null
              && !meta.getSequenceName().isEmpty()) {
            technicalKey =
                data.db.getNextSequenceValue(
                    data.realSchemaName, meta.getSequenceName(), f.getReturns().getKeyField());
            if (technicalKey != null && isRowLevel()) {
              logRowlevel(
                  BaseMessages.getString(PKG, "DimensionLookup.Log.FoundNextSequence2")
                      + technicalKey.toString());
            }
          } else {
            // Use our own sequence here...
            // What's the next value for the technical key?
            technicalKey =
                data.db.getNextValue(
                    data.realSchemaName, data.realTableName, f.getReturns().getKeyField());
          }

          // update our technicalKey with the return of the insert
          technicalKey =
              dimInsert(
                  rowMeta, row, technicalKey, false, valueNewVersion, valueDateFrom, valueDateTo);
          incrementLinesOutput();

          // We need to capture this change in the cache as well...
          if (meta.getCacheSize() >= 0) {
            Object[] values =
                getCacheValues(
                    rowMeta, row, technicalKey, valueNewVersion, valueDateFrom, valueDateTo);
            addToCache(lookupRow, values);
          }
        }
        if (punch) { // On of the fields we have to punch through has changed!
          /*
           * This means we have to update all versions:
           *
           * UPDATE dim SET punchf1 = val1, punchf2 = val2, ... WHERE fieldlookup[] = ?
           *
           * --> update ALL versions in the dimension table.
           */
          dimPunchThrough(rowMeta, row);
          incrementLinesUpdated();
        }

        returnRow = new Object[data.returnRowMeta.size()];
        returnRow[0] = technicalKey;
        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "DimensionLookup.Log.TechnicalKey") + technicalKey);
        }
      }
    }

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "DimensionLookup.Log.AddValuesToRow")
              + data.returnRowMeta.getString(returnRow));
    }

    // Copy the results to the output row...
    //
    // First copy the input row values to the output..
    //
    Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());

    int outputIndex = rowMeta.size();
    int inputIndex = 0;

    // Then the technical key...
    //
    if (data.returnRowMeta.getValueMeta(0).isBigNumber() && returnRow[0] instanceof Long) {
      if (isDebug()) {
        logDebug("Changing the type of the technical key from TYPE_BIGNUMBER to an TYPE_INTEGER");
      }
      IValueMeta tkValueMeta = data.returnRowMeta.getValueMeta(0);
      data.returnRowMeta.setValueMeta(
          0, ValueMetaFactory.cloneValueMeta(tkValueMeta, IValueMeta.TYPE_INTEGER));
    }

    outputRow[outputIndex++] = data.returnRowMeta.getInteger(returnRow, inputIndex++);

    // skip the version in the input
    inputIndex++;

    // Then get the "extra fields"...
    // don't return date from-to fields, they can be returned when explicitly
    // specified in lookup fields.
    while (inputIndex < returnRow.length && outputIndex < outputRow.length) {
      outputRow[outputIndex] = returnRow[inputIndex];
      outputIndex++;
      inputIndex++;
    }

    return outputRow;
  }

  /**
   * table: dimension table keys[]: which dim-fields do we use to look up key? retval: name of the
   * key to return datefield: do we have a datefield? datefrom, dateto: date-range, if any.
   */
  private void setDimLookup(IRowMeta rowMeta) throws HopException {
    DLFields f = meta.getFields();

    data.lookupRowMeta = new RowMeta();

    /*
     * DEFAULT, SYSDATE, START_PIPELINE, COLUMN_VALUE :
     *
     * SELECT <tk>, <version>, ... , FROM <table> WHERE key1=keys[1] AND key2=keys[2] ... AND <datefrom> <= <datefield>
     * AND <dateto> > <datefield>
     *
     * NULL :
     *
     * SELECT <tk>, <version>, ... , FROM <table> WHERE key1=keys[1] AND key2=keys[2] ... AND ( <datefrom> is null OR
     * <datefrom> <= <datefield> ) AND <dateto> >= <datefield>
     */
    StringBuilder sql = new StringBuilder();

    sql.append("SELECT ")
        .append(data.databaseMeta.quoteField(f.getReturns().getKeyField()))
        .append(", ")
        .append(data.databaseMeta.quoteField(f.getReturns().getVersionField()));

    for (DLField field : f.getFields()) {
      // Don't retrieve the fields without input
      if (StringUtils.isNotEmpty(field.getLookup())
          && isLookupOrUpdateTypeWithArgument(meta.isUpdate(), field)) {
        sql.append(", ").append(data.databaseMeta.quoteField(field.getLookup()));

        if (StringUtils.isNotEmpty(field.getName()) && !field.getLookup().equals(field.getName())) {
          sql.append(" AS ").append(data.databaseMeta.quoteField(field.getName()));
        }
      }
    }

    if (meta.getCacheSize() >= 0) {
      sql.append(", ")
          .append(data.databaseMeta.quoteField(f.getDate().getFrom()))
          .append(", ")
          .append(data.databaseMeta.quoteField(f.getDate().getTo()));
    }

    sql.append(" FROM ").append(data.schemaTable).append(CONST_WHERE);

    for (int i = 0; i < f.getKeys().size(); i++) {
      DLKey key = f.getKeys().get(i);
      if (i != 0) {
        sql.append(" AND ");
      }
      sql.append(data.databaseMeta.quoteField(key.getLookup())).append(" = ? ");
      data.lookupRowMeta.addValueMeta(rowMeta.getValueMeta(data.keynrs[i]));
    }

    String dateFromField = data.databaseMeta.quoteField(f.getDate().getFrom());
    String dateToField = data.databaseMeta.quoteField(f.getDate().getTo());

    if (meta.isUsingStartDateAlternative() && meta.getStartDateAlternative() == NULL
        || meta.getStartDateAlternative() == COLUMN_VALUE) {
      // Null as a start date is possible...
      //
      sql.append(" AND ( ")
          .append(dateFromField)
          .append(" IS NULL OR ")
          .append(dateFromField)
          .append(" <= ? )")
          .append(Const.CR);
      sql.append(" AND ").append(dateToField).append(" > ?").append(Const.CR);

      data.lookupRowMeta.addValueMeta(new ValueMetaDate(f.getDate().getFrom()));
      data.lookupRowMeta.addValueMeta(new ValueMetaDate(f.getDate().getTo()));
    } else {
      // Null as a start date is NOT possible
      //
      sql.append(" AND ? >= ").append(dateFromField).append(Const.CR);
      sql.append(" AND ? < ").append(dateToField).append(Const.CR);

      data.lookupRowMeta.addValueMeta(new ValueMetaDate(f.getDate().getFrom()));
      data.lookupRowMeta.addValueMeta(new ValueMetaDate(f.getDate().getTo()));
    }

    try {
      logDetailed("Dimension Lookup setting preparedStatement to [" + sql + "]");
      data.prepStatementLookup =
          data.db.getConnection().prepareStatement(data.databaseMeta.stripCR(sql));
      if (data.databaseMeta.supportsSetMaxRows()) {
        data.prepStatementLookup.setMaxRows(1); // alywas get only 1 line back!
      }
      if (data.databaseMeta.getIDatabase().isMySqlVariant()) {
        data.prepStatementLookup.setFetchSize(0); // Make sure to DISABLE Streaming Result sets
      }
      logDetailed("Finished preparing dimension lookup statement.");
    } catch (SQLException ex) {
      throw new HopDatabaseException("Unable to prepare dimension lookup", ex);
    }
  }

  protected boolean isAutoIncrement() {
    return meta.getFields().getReturns().getCreationMethod()
        == TechnicalKeyCreationMethod.AUTO_INCREMENT;
  }

  /**
   * This inserts new record into dimension Optionally, if the entry already exists, update date
   * range from previous version of the entry.
   */
  public Long dimInsert(
      IRowMeta inputRowMeta,
      Object[] row,
      Long technicalKey,
      boolean newEntry,
      Long versionNr,
      Date dateFrom,
      Date dateTo)
      throws HopException {
    DLFields f = meta.getFields();

    if (data.prepStatementInsert == null
        && data.prepStatementUpdate == null) { // first time: construct prepared statement
      IRowMeta insertRowMeta = new RowMeta();

      /*
       * Construct the SQL statement...
       *
       * INSERT INTO d_customer(keyfield, versionfield, datefrom, dateto, key[], fieldlookup[], last_updated,
       * last_inserted, last_version) VALUES (val_key ,val_version , val_datfrom, val_datto, keynrs[], fieldnrs[],
       * last_updated, last_inserted, last_version)
       */

      StringBuilder sql = new StringBuilder();

      sql.append("INSERT INTO ").append(data.schemaTable).append("( ");

      if (!isAutoIncrement()) {
        // NO AUTOINCREMENT
        sql.append(data.databaseMeta.quoteField(f.getReturns().getKeyField())).append(", ");
        insertRowMeta.addValueMeta(
            data.outputRowMeta.getValueMeta(inputRowMeta.size())); // the first return value
        // after the input
      } else {
        if (data.databaseMeta.needsPlaceHolder()) {
          sql.append("0, "); // placeholder on informix!
        }
      }

      sql.append(data.databaseMeta.quoteField(f.getReturns().getVersionField()))
          .append(", ")
          .append(data.databaseMeta.quoteField(f.getDate().getFrom()))
          .append(", ")
          .append(data.databaseMeta.quoteField(f.getDate().getTo()));
      insertRowMeta.addValueMeta(new ValueMetaInteger(f.getReturns().getVersionField()));
      insertRowMeta.addValueMeta(new ValueMetaDate(f.getDate().getFrom()));
      insertRowMeta.addValueMeta(new ValueMetaDate(f.getDate().getTo()));

      for (int i = 0; i < f.getKeys().size(); i++) {
        DLKey key = f.getKeys().get(i);
        sql.append(", ").append(data.databaseMeta.quoteField(key.getLookup()));
        insertRowMeta.addValueMeta(inputRowMeta.getValueMeta(data.keynrs[i]));
      }

      for (int i = 0; i < f.getFields().size(); i++) {
        DLField field = f.getFields().get(i);
        // Ignore last_version, last_updated etc, they are handled below (at the
        // back of the row).
        //
        if (meta.isUpdate() && field.getUpdateType().isWithArgument()) {
          sql.append(", ").append(data.databaseMeta.quoteField(field.getLookup()));
          insertRowMeta.addValueMeta(inputRowMeta.getValueMeta(data.fieldnrs[i]));
        }
      }

      // Finally, the special update fields...
      //
      for (int i = 0; i < f.getFields().size(); i++) {
        DLField field = f.getFields().get(i);
        IValueMeta valueMeta = null;
        switch (field.getUpdateType()) {
          case DATE_INSERTED_UPDATED, DATE_INSERTED:
            valueMeta = new ValueMetaDate(field.getLookup());
            break;
          case LAST_VERSION:
            valueMeta = new ValueMetaBoolean(field.getLookup());
            break;
          default:
            break;
        }
        if (valueMeta != null) {
          sql.append(", ").append(data.databaseMeta.quoteField(valueMeta.getName()));
          insertRowMeta.addValueMeta(valueMeta);
        }
      }

      sql.append(") VALUES (");

      if (!isAutoIncrement()) {
        sql.append("?, ");
      }
      sql.append("?, ?, ?");

      for (int i = 0; i < data.keynrs.length; i++) {
        sql.append(", ?");
      }

      for (DLField field : f.getFields()) {
        // Ignore last_version, last_updated, etc. These are handled below...
        //
        if (meta.isUpdate() && field.getUpdateType().isWithArgument()) {
          sql.append(", ?");
        }
      }

      // The special update fields...
      //
      for (DLField field : f.getFields()) {
        switch (field.getUpdateType()) {
          case DATE_INSERTED_UPDATED, DATE_INSERTED, LAST_VERSION:
            sql.append(", ?");
            break;
          default:
            break;
        }
      }

      sql.append(" )");

      try {
        if (technicalKey == null && data.databaseMeta.supportsAutoGeneratedKeys()) {
          logDetailed("SQL w/ return keys=[" + sql + "]");
          data.prepStatementInsert =
              data.db
                  .getConnection()
                  .prepareStatement(
                      data.databaseMeta.stripCR(sql), Statement.RETURN_GENERATED_KEYS);
        } else {
          logDetailed("SQL=[" + sql + "]");
          data.prepStatementInsert =
              data.db.getConnection().prepareStatement(data.databaseMeta.stripCR(sql));
        }
      } catch (SQLException ex) {
        throw new HopDatabaseException("Unable to prepare dimension insert :" + Const.CR + sql, ex);
      }

      /*
       * UPDATE d_customer SET dateto = val_datnow, last_updated = <now> last_version = false WHERE keylookup[] =
       * keynrs[] AND versionfield = val_version - 1
       */
      IRowMeta updateRowMeta = new RowMeta();

      StringBuilder sqlUpdate = new StringBuilder();
      sqlUpdate.append(CONST_UPDATE).append(data.schemaTable).append(Const.CR);

      // The end of the date range
      //
      sqlUpdate
          .append("SET ")
          .append(data.databaseMeta.quoteField(f.getDate().getTo()))
          .append(" = ?")
          .append(Const.CR);
      updateRowMeta.addValueMeta(new ValueMetaDate(f.getDate().getTo()));

      // The special update fields...
      //
      for (DLField field : f.getFields()) {
        IValueMeta valueMeta = null;
        switch (field.getUpdateType()) {
          case DATE_INSERTED_UPDATED, DATE_UPDATED:
            valueMeta = new ValueMetaDate(field.getLookup());
            break;
          case LAST_VERSION:
            valueMeta = new ValueMetaBoolean(field.getLookup());
            break;
          default:
            break;
        }
        if (valueMeta != null) {
          sqlUpdate
              .append(", ")
              .append(data.databaseMeta.quoteField(valueMeta.getName()))
              .append(" = ?")
              .append(Const.CR);
          updateRowMeta.addValueMeta(valueMeta);
        }
      }

      sqlUpdate.append("WHERE ");
      for (int i = 0; i < f.getKeys().size(); i++) {
        DLKey key = f.getKeys().get(i);
        if (i > 0) {
          sqlUpdate.append(CONST_AND);
        }
        sqlUpdate
            .append(data.databaseMeta.quoteField(key.getLookup()))
            .append(" = ?")
            .append(Const.CR);
        updateRowMeta.addValueMeta(inputRowMeta.getValueMeta(data.keynrs[i]));
      }
      sqlUpdate
          .append(CONST_AND)
          .append(data.databaseMeta.quoteField(f.getReturns().getVersionField()))
          .append(" = ? ");
      updateRowMeta.addValueMeta(new ValueMetaInteger(f.getReturns().getVersionField()));

      try {
        logDetailed("Preparing update: " + Const.CR + sqlUpdate + Const.CR);
        data.prepStatementUpdate =
            data.db.getConnection().prepareStatement(data.databaseMeta.stripCR(sqlUpdate));
      } catch (SQLException ex) {
        throw new HopDatabaseException(
            "Unable to prepare dimension update :" + Const.CR + sqlUpdate, ex);
      }

      data.insertRowMeta = insertRowMeta;
      data.updateRowMeta = updateRowMeta;
    }

    Object[] insertRow = new Object[data.insertRowMeta.size()];
    int insertIndex = 0;
    if (!isAutoIncrement()) {
      insertRow[insertIndex++] = technicalKey;
    }

    // Caller is responsible for setting proper version number depending
    // on if newEntry == true
    insertRow[insertIndex++] = versionNr;

    switch (data.startDateAlternative) {
      case NONE:
        insertRow[insertIndex++] = dateFrom;
        break;
      case SYSTEM_DATE:
        // use the time the transform execution begins as the date from (passed in as dateFrom).
        // before, the current system time was used. this caused an exclusion of the row in the
        // lookup portion of the transform that uses this 'valueDate' and not the current time.
        // the result was multiple inserts for what should have been 1
        insertRow[insertIndex++] = dateFrom;
        break;
      case PIPELINE_START:
        insertRow[insertIndex++] = getPipeline().getExecutionStartDate();
        break;
      case NULL:
        insertRow[insertIndex++] = null;
        break;
      case COLUMN_VALUE:
        insertRow[insertIndex++] = inputRowMeta.getDate(row, data.startDateFieldIndex);
        break;
      default:
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "DimensionLookup.Exception.IllegalStartDateSelection",
                data.startDateAlternative.getDescription()));
    }

    insertRow[insertIndex++] = dateTo;

    for (int i = 0; i < data.keynrs.length; i++) {
      insertRow[insertIndex++] = row[data.keynrs[i]];
    }
    for (int i = 0; i < data.fieldnrs.length; i++) {
      if (data.fieldnrs[i] >= 0) {
        // Ignore last_version, last_updated, etc. These are handled below...
        //
        insertRow[insertIndex++] = row[data.fieldnrs[i]];
      }
    }
    // The special update fields...
    //
    for (DLField field : f.getFields()) {
      switch (field.getUpdateType()) {
        case DATE_INSERTED_UPDATED, DATE_INSERTED:
          insertRow[insertIndex++] = new Date();
          break;
        case LAST_VERSION:
          insertRow[insertIndex++] = Boolean.TRUE;
          break; // Always the last version on insert.
        default:
          break;
      }
    }

    if (isDebug()) {
      logDebug(
          "rins, size="
              + data.insertRowMeta.size()
              + ", values="
              + data.insertRowMeta.getString(insertRow));
    }

    // INSERT NEW VALUE!
    data.db.setValues(data.insertRowMeta, insertRow, data.prepStatementInsert);
    data.db.insertRow(data.prepStatementInsert);

    if (isDebug()) {
      logDebug("Row inserted!");
    }
    if (technicalKey == null && data.databaseMeta.supportsAutoGeneratedKeys()) {
      try {
        RowMetaAndData keys = data.db.getGeneratedKeys(data.prepStatementInsert);
        if (!keys.getRowMeta().isEmpty()) {
          technicalKey = keys.getRowMeta().getInteger(keys.getData(), 0);
        } else {
          throw new HopDatabaseException(
              "Unable to retrieve value of auto-generated technical key : no value found!");
        }
      } catch (Exception e) {
        throw new HopDatabaseException(
            "Unable to retrieve value of auto-generated technical key : unexpected error: ", e);
      }
    }

    if (!newEntry) { // we have to update the previous version in the dimension!
      /*
       * UPDATE d_customer SET dateto = val_datfrom , last_updated = <now> , last_version = false WHERE keylookup[] =
       * keynrs[] AND versionfield = val_version - 1
       */
      Object[] updateRow = new Object[data.updateRowMeta.size()];
      int updateIndex = 0;

      switch (data.startDateAlternative) {
        case NONE:
          updateRow[updateIndex++] = dateFrom;
          break;
        case SYSTEM_DATE:
          updateRow[updateIndex++] = new Date();
          break;
        case PIPELINE_START:
          updateRow[updateIndex++] = getPipeline().getExecutionStartDate();
          break;
        case NULL:
          updateRow[updateIndex++] = null;
          break;
        case COLUMN_VALUE:
          updateRow[updateIndex++] = inputRowMeta.getDate(row, data.startDateFieldIndex);
          break;
        default:
          throw new HopTransformException(
              BaseMessages.getString(
                  "DimensionLookup.Exception.IllegalStartDateSelection",
                  data.startDateAlternative.getDescription()));
      }

      // The special update fields...
      //
      for (DLField field : f.getFields()) {
        switch (field.getUpdateType()) {
          case DATE_INSERTED_UPDATED, DATE_UPDATED:
            updateRow[updateIndex++] = new Date();
            break;
          case LAST_VERSION:
            updateRow[updateIndex++] = Boolean.FALSE;
            break; // Never the last version on this update
          default:
            break;
        }
      }

      for (int i = 0; i < data.keynrs.length; i++) {
        updateRow[updateIndex++] = row[data.keynrs[i]];
      }

      updateRow[updateIndex] = versionNr - 1;

      if (isRowLevel()) {
        logRowlevel("UPDATE using rupd=" + data.updateRowMeta.getString(updateRow));
      }

      // UPDATE VALUES

      // set values for update
      //
      data.db.setValues(data.updateRowMeta, updateRow, data.prepStatementUpdate);
      if (isDebug()) {
        logDebug("Values set for update (" + data.updateRowMeta.size() + ")");
      }
      data.db.insertRow(data.prepStatementUpdate); // do the actual update
      if (isDebug()) {
        logDebug("Row updated!");
      }
    }

    return technicalKey;
  }

  public void dimUpdate(IRowMeta rowMeta, Object[] row, Long dimkey, Date valueDate)
      throws HopDatabaseException {
    DLFields f = meta.getFields();

    if (data.prepStatementDimensionUpdate == null) {
      // first time: construct prepared statement
      //
      data.dimensionUpdateRowMeta = new RowMeta();

      // Construct the SQL statement...
      /*
       * UPDATE d_customer SET fieldlookup[] = row.getValue(fieldnrs) , last_updated = <now> WHERE returnkey = dimkey
       */

      StringBuilder sql = new StringBuilder();

      sql.append(CONST_UPDATE).append(data.schemaTable).append(Const.CR).append("SET ");
      boolean comma = false;
      for (int i = 0; i < f.getFields().size(); i++) {
        DLField field = f.getFields().get(i);
        if (meta.isUpdate() && field.getUpdateType().isWithArgument()) {
          if (comma) {
            sql.append(", ");
          } else {
            sql.append("  ");
          }
          comma = true;
          sql.append(data.databaseMeta.quoteField(field.getLookup()))
              .append(" = ?")
              .append(Const.CR);
          data.dimensionUpdateRowMeta.addValueMeta(rowMeta.getValueMeta(data.fieldnrs[i]));
        }
      }

      // The special update fields...
      //
      for (DLField field : f.getFields()) {
        IValueMeta valueMeta = null;
        switch (field.getUpdateType()) {
          case DATE_INSERTED_UPDATED, DATE_UPDATED:
            valueMeta = new ValueMetaDate(field.getLookup());
            break;
          default:
            break;
        }
        if (valueMeta != null) {
          if (comma) {
            sql.append(", ");
          } else {
            sql.append("  ");
          }
          comma = true;
          sql.append(data.databaseMeta.quoteField(valueMeta.getName()))
              .append(" = ?")
              .append(Const.CR);
          data.dimensionUpdateRowMeta.addValueMeta(valueMeta);
        }
      }

      sql.append("WHERE  ")
          .append(data.databaseMeta.quoteField(f.getReturns().getKeyField()))
          .append(" = ?");
      data.dimensionUpdateRowMeta.addValueMeta(
          new ValueMetaInteger(f.getReturns().getKeyField())); // The tk

      try {
        if (isDebug()) {
          logDebug("Preparing statement: [" + sql + "]");
        }
        data.prepStatementDimensionUpdate =
            data.db.getConnection().prepareStatement(data.databaseMeta.stripCR(sql));
      } catch (SQLException ex) {
        throw new HopDatabaseException("Couldn't prepare statement :" + Const.CR + sql, ex);
      }
    }

    // Assemble information
    // New
    Object[] dimensionUpdateRow = new Object[data.dimensionUpdateRowMeta.size()];
    int updateIndex = 0;
    for (int i = 0; i < data.fieldnrs.length; i++) {
      // Ignore last_version, last_updated, etc. These are handled below...
      //
      if (data.fieldnrs[i] >= 0) {
        dimensionUpdateRow[updateIndex++] = row[data.fieldnrs[i]];
      }
    }
    for (DLField field : f.getFields()) {
      switch (field.getUpdateType()) {
        case DATE_INSERTED_UPDATED, DATE_UPDATED:
          dimensionUpdateRow[updateIndex++] = valueDate;
          break;
        default:
          break;
      }
    }
    dimensionUpdateRow[updateIndex] = dimkey;

    data.db.setValues(
        data.dimensionUpdateRowMeta, dimensionUpdateRow, data.prepStatementDimensionUpdate);
    data.db.insertRow(data.prepStatementDimensionUpdate);
  }

  // This updates all versions of a dimension entry.
  //
  public void dimPunchThrough(IRowMeta rowMeta, Object[] row) throws HopDatabaseException {
    DLFields f = meta.getFields();
    if (data.prepStatementPunchThrough == null) { // first time: construct prepared statement
      data.punchThroughRowMeta = new RowMeta();

      /*
       * UPDATE table SET punchv1 = fieldx, ... , last_updated = <now> WHERE keylookup[] = keynrs[]
       */

      StringBuilder sqlUpdate = new StringBuilder();
      sqlUpdate.append(CONST_UPDATE).append(data.schemaTable).append(Const.CR);
      sqlUpdate.append("SET ");
      boolean first = true;
      for (int i = 0; i < f.getFields().size(); i++) {
        DLField field = f.getFields().get(i);
        if (field.getUpdateType() == PUNCH_THROUGH) {
          if (!first) {
            sqlUpdate.append(", ");
          } else {
            sqlUpdate.append("  ");
          }
          first = false;
          sqlUpdate
              .append(data.databaseMeta.quoteField(field.getLookup()))
              .append(" = ?")
              .append(Const.CR);
          data.punchThroughRowMeta.addValueMeta(rowMeta.getValueMeta(data.fieldnrs[i]));
        }
      }

      // The special update fields...
      //
      for (DLField field : f.getFields()) {
        IValueMeta valueMeta = null;
        switch (field.getUpdateType()) {
          case DATE_INSERTED_UPDATED, DATE_UPDATED:
            valueMeta = new ValueMetaDate(field.getLookup());
            break;
          default:
            break;
        }
        if (valueMeta != null) {
          sqlUpdate
              .append(", ")
              .append(data.databaseMeta.quoteField(valueMeta.getName()))
              .append(" = ?")
              .append(Const.CR);
          data.punchThroughRowMeta.addValueMeta(valueMeta);
        }
      }

      sqlUpdate.append("WHERE ");
      for (int i = 0; i < f.getKeys().size(); i++) {
        DLKey key = f.getKeys().get(i);
        if (i > 0) {
          sqlUpdate.append(CONST_AND);
        }
        sqlUpdate
            .append(data.databaseMeta.quoteField(key.getLookup()))
            .append(" = ?")
            .append(Const.CR);
        data.punchThroughRowMeta.addValueMeta(rowMeta.getValueMeta(data.keynrs[i]));
      }

      try {
        data.prepStatementPunchThrough =
            data.db.getConnection().prepareStatement(data.databaseMeta.stripCR(sqlUpdate));
      } catch (SQLException ex) {
        throw new HopDatabaseException(
            "Unable to prepare dimension punchThrough update statement : " + Const.CR + sqlUpdate,
            ex);
      }
    }

    Object[] punchThroughRow = new Object[data.punchThroughRowMeta.size()];
    int punchIndex = 0;

    for (int i = 0; i < f.getFields().size(); i++) {
      DLField field = f.getFields().get(i);
      switch (field.getUpdateType()) {
        case DATE_INSERTED_UPDATED, DATE_UPDATED:
          punchThroughRow[punchIndex++] = new Date();
          break;
        case PUNCH_THROUGH:
          punchThroughRow[punchIndex++] = row[data.fieldnrs[i]];
          break;
        default:
          break;
      }
    }

    for (int i = 0; i < data.keynrs.length; i++) {
      punchThroughRow[punchIndex++] = row[data.keynrs[i]];
    }

    // UPDATE VALUES
    data.db.setValues(
        data.punchThroughRowMeta,
        punchThroughRow,
        data.prepStatementPunchThrough); // set values for
    // update
    data.db.insertRow(data.prepStatementPunchThrough); // do the actual punch through update
  }

  /**
   * Keys: - natural key fields Values: - Technical key - lookup fields / extra fields (allows us to
   * compare or retrieve) - Date_from - Date_to
   *
   * @param row The input row
   * @param technicalKey the technical key value
   * @param valueDateFrom the start of valid date range
   * @param valueDateTo the end of the valid date range
   * @return the values to store in the cache as a row.
   */
  private Object[] getCacheValues(
      IRowMeta rowMeta,
      Object[] row,
      Long technicalKey,
      Long valueVersion,
      Date valueDateFrom,
      Date valueDateTo) {
    if (data.cacheValueRowMeta == null) {
      return null; // nothing is in the cache.
    }

    Object[] cacheValues = new Object[data.cacheValueRowMeta.size()];
    int cacheIndex = 0;

    cacheValues[cacheIndex++] = technicalKey;

    cacheValues[cacheIndex++] = valueVersion;

    for (int i = 0; i < data.fieldnrs.length; i++) {
      // Ignore last_version, last_updated, etc. These are handled below...
      //
      if (data.fieldnrs[i] >= 0) {
        cacheValues[cacheIndex++] = row[data.fieldnrs[i]];
      }
    }

    cacheValues[cacheIndex++] = valueDateFrom;
    cacheValues[cacheIndex] = valueDateTo;

    return cacheValues;
  }

  /**
   * Adds a row to the cache In case we are doing updates, we need to store the complete rows from
   * the database. These are the values we need to store
   *
   * <p>Key: - natural key fields Value: - Technical key - lookup fields / extra fields (allows us
   * to compare or retrieve) - Date_from - Date_to
   *
   * @param keyValues
   * @param returnValues
   * @throws HopValueException
   */
  private void addToCache(Object[] keyValues, Object[] returnValues) throws HopValueException {
    if (data.cacheValueRowMeta == null) {
      data.cacheValueRowMeta = assembleCacheValueRowMeta();
    }

    // store it in the cache if needed.
    byte[] keyPart = RowMeta.extractData(data.cacheKeyRowMeta, keyValues);
    byte[] valuePart = RowMeta.extractData(data.cacheValueRowMeta, returnValues);
    data.cache.put(keyPart, valuePart);

    // check if the size is not too big...
    // Allow for a buffer overrun of 20% and then remove those 20% in one go.
    // Just to keep performance in track.
    //
    int tenPercent = meta.getCacheSize() / 10;
    if (meta.getCacheSize() > 0 && data.cache.size() > meta.getCacheSize() + tenPercent) {
      // Which cache entries do we delete here?
      // We delete those with the lowest technical key...
      // Those would arguably be the "oldest" dimension entries.
      // Oh well... Nothing is going to be perfect here...
      //
      // Getting the lowest 20% requires some kind of sorting algorithm and I'm not sure we want to
      // do that.
      // Sorting is slow and even in the best case situation we need to do 2 passes over the cache
      // entries...
      //
      // Perhaps we should get 20% random values and delete everything below the lowest but one TK.
      //
      List<byte[]> keys = data.cache.getKeys();
      int sizeBefore = keys.size();
      List<Long> samples = new ArrayList<>();

      // Take 10 sample technical keys....
      int transformsize = keys.size() / 5;
      if (transformsize < 1) {
        transformsize = 1; // make shure we have no endless loop
      }
      for (int i = 0; i < keys.size(); i += transformsize) {
        byte[] key = keys.get(i);
        byte[] value = data.cache.get(key);
        if (value != null) {
          Object[] values = RowMeta.getRow(data.cacheValueRowMeta, value);
          Long tk = data.cacheValueRowMeta.getInteger(values, 0);
          samples.add(tk);
        }
      }
      // Sort these 5 elements...
      Collections.sort(samples);

      // What is the smallest?
      // Take the second, not the fist in the list, otherwise we would be removing a single entry =
      // not good.
      if (samples.size() > 1) {
        data.smallestCacheKey = samples.get(1);
      } else if (!samples.isEmpty()) { // except when there is only one sample
        data.smallestCacheKey = samples.get(0);
      } else {
        // If no samples found nothing to remove, we're done
        return;
      }

      // Remove anything in the cache <= smallest.
      // This makes it almost single pass...
      // This algorithm is not 100% correct, but I guess it beats sorting the whole cache all the
      // time.
      //
      for (int i = 0; i < keys.size(); i++) {
        byte[] key = keys.get(i);
        byte[] value = data.cache.get(key);
        if (value != null) {
          Object[] values = RowMeta.getRow(data.cacheValueRowMeta, value);
          long tk = data.cacheValueRowMeta.getInteger(values, 0).longValue();
          if (tk <= data.smallestCacheKey) {
            data.cache.remove(key); // this one has to go.
          }
        }
      }

      int sizeAfter = data.cache.size();
      logDetailed("Reduced the lookup cache from " + sizeBefore + " to " + sizeAfter + " rows.");
    }

    if (isRowLevel()) {
      logRowlevel(
          "Cache store: key="
              + Arrays.toString(keyValues)
              + "    values="
              + Arrays.toString(returnValues));
    }
  }

  /**
   * @return the cache value row metadata. The items that are cached is basically the return row
   *     metadata:<br>
   *     - Technical key (Integer) - Version (Integer) -
   */
  private IRowMeta assembleCacheValueRowMeta() {
    // The technical key and version are always an Integer...
    return data.returnRowMeta.clone();
  }

  private Object[] getFromCache(Object[] keyValues, Date dateValue) throws HopValueException {
    if (data.cacheValueRowMeta == null) {
      // nothing in the cache yet, no lookup was ever performed
      if (data.returnRowMeta == null) {
        return null;
      }

      data.cacheValueRowMeta = assembleCacheValueRowMeta();
    }

    byte[] key = RowMeta.extractData(data.cacheKeyRowMeta, keyValues);
    byte[] value = data.cache.get(key);
    if (value != null) {
      Object[] row = RowMeta.getRow(data.cacheValueRowMeta, value);

      // See if the dateValue is between the from and to date ranges...
      // The last 2 values are from and to
      long time = dateValue.getTime();
      long from = 0L;
      long to = 0L;

      Date dateFrom = (Date) row[row.length - 2];
      if (dateFrom != null) {
        from = dateFrom.getTime();
      }
      Date dateTo = (Date) row[row.length - 1];
      if (dateTo != null) {
        to = dateTo.getTime();
      }

      if (time >= from && time < to) { // sanity check to see if we have the right version
        if (isRowLevel()) {
          logRowlevel(
              "Cache hit: key="
                  + data.cacheKeyRowMeta.getString(keyValues)
                  + "  values="
                  + data.cacheValueRowMeta.getString(row));
        }
        return row;
      }
    }
    return null;
  }

  public void checkDimZero() throws HopException {
    // Manually disabled
    if (meta.isUnknownRowCheckDisabled()) {
      return;
    }
    // Don't insert anything when running in lookup mode.
    //
    if (!meta.isUpdate()) {
      return;
    }
    DLFields f = meta.getFields();
    int startTechnicalKey = data.databaseMeta.getNotFoundTK(isAutoIncrement());

    if (isAutoIncrement()) {
      // See if there are rows in the table
      // If so, we can't insert the unknown row anymore...
      //
      String sql =
          "SELECT count(*) FROM "
              + data.schemaTable
              + CONST_WHERE
              + data.databaseMeta.quoteField(f.getReturns().getKeyField())
              + " = "
              + startTechnicalKey;
      RowMetaAndData r = data.db.getOneRow(sql);
      Long count = r.getRowMeta().getInteger(r.getData(), 0);
      if (count.longValue() != 0) {
        return; // Can't insert below the rows already in there...
      }
    }

    String sql =
        "SELECT count(*) FROM "
            + data.schemaTable
            + CONST_WHERE
            + data.databaseMeta.quoteField(f.getReturns().getKeyField())
            + " = "
            + startTechnicalKey;
    RowMetaAndData r = data.db.getOneRow(sql);
    Long count = r.getRowMeta().getInteger(r.getData(), 0);
    if (count.longValue() == 0) {
      String isql = null;
      try {
        if (!data.databaseMeta.supportsAutoinc() || !isAutoIncrement()) {
          isql =
              "insert into "
                  + data.schemaTable
                  + "("
                  + data.databaseMeta.quoteField(f.getReturns().getKeyField())
                  + ", "
                  + data.databaseMeta.quoteField(f.getReturns().getVersionField())
                  + ") values (0, 1)";
        } else {
          isql =
              data.databaseMeta.getSqlInsertAutoIncUnknownDimensionRow(
                  data.schemaTable,
                  data.databaseMeta.quoteField(f.getReturns().getKeyField()),
                  data.databaseMeta.quoteField(f.getReturns().getVersionField()));
        }

        data.db.execStatement(data.databaseMeta.stripCR(isql));
      } catch (HopException e) {
        throw new HopDatabaseException(
            "Error inserting 'unknown' row in dimension [" + data.schemaTable + "] : " + isql, e);
      }
    }
  }

  @Override
  public boolean init() {

    if (!super.init()) {
      return false;
    }

    data.minDate = meta.getMinDate();
    data.maxDate = meta.getMaxDate();

    data.realSchemaName = resolve(meta.getSchemaName());
    data.realTableName = resolve(meta.getTableName());

    if (meta.isUsingStartDateAlternative()) {
      data.startDateAlternative = meta.getStartDateAlternative();
    }
    if (data.startDateAlternative == null) {
      data.startDateAlternative = NONE;
    }

    data.databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

    if (data.databaseMeta == null) {
      logError(
          BaseMessages.getString(
              PKG, "DimensionLookup.Init.ConnectionMissing", getTransformName()));
      return false;
    }
    data.db = new Database(this, this, data.databaseMeta);
    try {
      data.db.connect();

      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "DimensionLookup.Log.ConnectedToDB"));
      }
      data.db.setCommit(meta.getCommitSize());

      return true;
    } catch (HopException ke) {
      logError(
          BaseMessages.getString(PKG, "DimensionLookup.Log.ErrorOccurredInProcessing")
              + ke.getMessage());
    }

    return false;
  }

  @Override
  public void dispose() {
    if (data.db != null) {
      try {
        if (!data.db.isAutoCommit()) {
          if (getErrors() == 0) {
            data.db.commit();
          } else {
            data.db.rollback();
          }
        }
      } catch (HopDatabaseException e) {
        logError(
            BaseMessages.getString(PKG, "DimensionLookup.Log.ErrorOccurredInProcessing")
                + e.getMessage());
      } finally {
        data.db.disconnect();
      }
    }
    super.dispose();
  }
}
