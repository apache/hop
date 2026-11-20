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

package org.apache.hop.pipeline.transforms.databaselookup;

import com.google.common.annotations.VisibleForTesting;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.databaselookup.readallcache.ReadAllCache;

/** Looks up values in a database using keys from input streams. */
public class DatabaseLookup extends BaseTransform<DatabaseLookupMeta, DatabaseLookupData> {

  private static final Class<?> PKG = DatabaseLookupMeta.class;

  /**
   * Cached for a key that the database does not have, when such a row is dropped or sent to the
   * error stream (a miss that falls back to the default values caches those values instead).
   */
  static final Object[] NOT_FOUND = new Object[0];

  public DatabaseLookup(
      TransformMeta transformMeta,
      DatabaseLookupMeta meta,
      DatabaseLookupData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Performs the lookup based on the meta-data and the input row.
   *
   * @param row The row to use as lookup data and the row to add the returned lookup fields to
   * @return the resulting row after the lookup values where added
   * @throws HopException In case something goes wrong.
   */
  @VisibleForTesting
  synchronized Object[] lookupValues(IRowMeta inputRowMeta, Object[] row) throws HopException {
    Object[] outputRow = RowDataUtil.resizeArray(row, data.outputRowMeta.size());

    Object[] lookupRow = buildLookupRow(inputRowMeta, row);

    Object[] add;
    boolean cacheNow = false;
    boolean cacheHit = false;
    boolean knownMiss = false;

    // First, check if we looked up before
    if (meta.isCached()) {
      add = data.cache.getRowFromCache(data.lookupMeta, lookupRow);
      if (add == NOT_FOUND) {
        // looked up before and found nothing: no need to ask the database again
        knownMiss = true;
        add = null;
      } else if (add != null) {
        cacheHit = true;
      }
    } else {
      add = null;
    }

    if (add == null
        && !knownMiss
        && (!(meta.isCached() && meta.isLoadingAllDataInCache()) || data.hasDBCondition)) {
      // do not go to the
      // database when all rows
      // are in (exception LIKE
      // operator)
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "DatabaseLookup.Log.AddedValuesToLookupRow1")
                + meta.getLookup().getKeyFields().size()
                + BaseMessages.getString(PKG, "DatabaseLookup.Log.AddedValuesToLookupRow2")
                + data.lookupMeta.getString(lookupRow));
      }

      data.db.setValuesLookup(data.lookupMeta, lookupRow);
      add = data.db.getLookup(meta.getLookup().isFailingOnMultipleResults());
      cacheNow = true;
    }

    // Store in cache if we need to!
    // If we already loaded all data into the cache, storing more makes no sense.
    //
    boolean storeInCache =
        meta.isCached() && cacheNow && !meta.isLoadingAllDataInCache() && data.allEquals;

    if (add == null) { // nothing was found, unknown code: add default values
      if (meta.getLookup().isEatingRowOnLookupFailure()) {
        if (storeInCache) {
          data.cache.storeRowInCache(meta, data.lookupMeta, lookupRow.clone(), NOT_FOUND);
        }
        return null;
      }
      if (getTransformMeta().isDoingErrorHandling()) {
        if (storeInCache) {
          data.cache.storeRowInCache(meta, data.lookupMeta, lookupRow.clone(), NOT_FOUND);
        }
        putError(
            getInputRowMeta(),
            row,
            1L,
            buildLookupFailureDescription(lookupRow),
            String.join(", ", data.lookupMeta.getFieldNames()),
            "DBL001");

        // return false else we would still be processed.
        return null;
      }

      if (isRowLevel()) {
        logRowlevel(BaseMessages.getString(PKG, "DatabaseLookup.Log.NoResultsFoundAfterLookup"));
      }

      add = new Object[data.returnMeta.size()];
      for (int i = 0; i < meta.getLookup().getReturnValues().size(); i++) {
        if (data.nullif[i] != null) {
          add[i] = data.nullif[i];
        } else {
          add[i] = null;
        }
      }

      if (storeInCache) {
        // A cache hit is trimmed (see below) while this first miss is not: keep it that way.
        Object[] cached = add.clone();
        trimReturnValues(cached);
        data.cache.storeRowInCache(meta, data.lookupMeta, lookupRow.clone(), cached);
      }
    } else {
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "DatabaseLookup.Log.FoundResultsAfterLookup")
                + Arrays.toString(add));
      }

      // Only trim and verify the data types if the data comes from the DB, NOT when we have a
      // cache hit: rows go into the cache trimmed (and converted, when they came from a lookup
      // query), so there is nothing left to do for them.
      if (!cacheHit) {
        // Trim the fields if required
        trimReturnValues(add);

        incrementLinesInput();

        // The assumption here is that the types are in the same order
        // as the returned lookup row, but since we make the lookup row
        // that should not be a problem.
        //
        int[] types = data.returnValueTypes;
        for (int i = 0; i < types.length; i++) {
          IValueMeta returned = data.db.getReturnRowMeta().getValueMeta(i);
          IValueMeta expected = data.returnMeta.getValueMeta(i);

          if (returned != null && types[i] > 0 && types[i] != returned.getType()) {
            // Set the type to the default return type
            add[i] = expected.convertData(returned, add[i]);
          }
        }

        if (storeInCache) {
          data.cache.storeRowInCache(meta, data.lookupMeta, lookupRow.clone(), add);
        }
      }
    }

    for (int i = 0; i < data.returnMeta.size(); i++) {
      outputRow[inputRowMeta.size() + i] = add[i];
    }

    return outputRow;
  }

  /**
   * Fills the (reused) lookup row from the stream row, converting each key to the type of its table
   * column where needed. The returned array is only valid until the next call: copy it to keep it.
   */
  private Object[] buildLookupRow(IRowMeta inputRowMeta, Object[] row) throws HopValueException {
    if (data.lookupSourceIndexes == null) {
      prepareLookupRow(inputRowMeta);
    }
    int[] sources = data.lookupSourceIndexes;
    boolean[] conversion = data.lookupConversion;
    Object[] lookupRow = data.lookupRowBuffer;

    for (int i = 0; i < sources.length; i++) {
      Object value = row[sources[i]];
      if (conversion[i]) {
        IValueMeta valueMeta = data.lookupMeta.getValueMeta(i);
        value = valueMeta.convertData(inputRowMeta.getValueMeta(sources[i]), value);
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
      }
      lookupRow[i] = value;
    }
    return lookupRow;
  }

  /** Works out once which stream field feeds each lookup slot and whether it needs converting. */
  private void prepareLookupRow(IRowMeta inputRowMeta) {
    List<Integer> sources = new ArrayList<>();
    for (int i = 0; i < meta.getLookup().getKeyFields().size(); i++) {
      if (data.keynrs[i] >= 0) {
        sources.add(data.keynrs[i]);
      }
      if (data.keynrs2[i] >= 0) {
        sources.add(data.keynrs2[i]);
      }
    }
    int[] indexes = new int[sources.size()];
    boolean[] conversion = new boolean[sources.size()];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = sources.get(i);
      IValueMeta input = inputRowMeta.getValueMeta(indexes[i]);
      IValueMeta value = data.lookupMeta.getValueMeta(i);
      conversion[i] =
          input.getType() != value.getType()
              || IValueMeta.STORAGE_TYPE_BINARY_STRING == input.getStorageType();
    }
    data.lookupConversion = conversion;
    data.lookupRowBuffer = new Object[data.lookupMeta.size()];
    data.lookupSourceIndexes = indexes;
  }

  /** Trims the string return values that have a trim type set, in place. */
  private void trimReturnValues(Object[] values) throws HopValueException {
    for (int i : data.trimIndexes) {
      IValueMeta expected = data.returnMeta.getValueMeta(i);
      values[i] =
          expected.convertDataFromString(
              (String) values[i],
              expected,
              "",
              "",
              ValueMetaBase.getTrimTypeByCode(data.returnTrimTypes[i]));
    }
  }

  // visible for testing purposes
  void determineFieldsTypesQueryingDb() throws HopException {
    List<KeyField> keyFields = meta.getLookup().getKeyFields();
    data.keytypes = new int[keyFields.size()];

    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
    String schemaTable =
        databaseMeta.getQuotedSchemaTableCombination(
            this, meta.getSchemaName(), meta.getTableName());

    IRowMeta fields = data.db.getTableFields(schemaTable);
    data.dbRowMeta = fields;
    if (fields != null) {
      // Fill in the types...
      for (int i = 0; i < keyFields.size(); i++) {
        KeyField keyField = keyFields.get(i);
        IValueMeta key = fields.searchValueMeta(keyField.getTableField());
        if (key != null) {
          data.keytypes[i] = key.getType();
        } else {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "DatabaseLookup.ERROR0001.FieldRequired5.Exception")
                  + keyField.getTableField()
                  + BaseMessages.getString(
                      PKG, "DatabaseLookup.ERROR0001.FieldRequired6.Exception"));
        }
      }
    } else {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DatabaseLookup.ERROR0002.UnableToDetermineFieldsOfTable")
              + schemaTable
              + "]");
    }
  }

  private void initNullIf() throws HopException {
    final List<ReturnValue> returnValues = meta.getLookup().getReturnValues();

    data.nullif = new Object[returnValues.size()];

    for (int i = 0; i < returnValues.size(); i++) {
      ReturnValue returnValue = returnValues.get(i);
      if (StringUtils.isNotEmpty(returnValue.getDefaultValue())) {
        IValueMeta stringMeta = new ValueMetaString("string");
        IValueMeta returnMeta = data.outputRowMeta.getValueMeta(i + getInputRowMeta().size());
        data.nullif[i] = returnMeta.convertData(stringMeta, returnValue.getDefaultValue());
      } else {
        data.nullif[i] = null;
      }
    }
  }

  /**
   * Describes a lookup that found nothing. The description ends up both on the error stream, as the
   * error description field, and in the log at debug level, so it has to name the table that was
   * queried and the key values that missed - a bare "No lookup found" repeated per row says
   * nothing. It is deliberately not translated: it is a value on the error stream that pipelines
   * filter on, so it has to read the same in every locale.
   */
  private String buildLookupFailureDescription(Object[] lookupRow) {
    if (data.lookupFailurePrefix == null) {
      StringBuilder prefix = new StringBuilder("No lookup found");
      String table = resolve(meta.getTableName());
      if (!Utils.isEmpty(table)) {
        prefix.append(" in ");
        String schema = resolve(meta.getSchemaName());
        if (!Utils.isEmpty(schema)) {
          prefix.append(schema).append('.');
        }
        prefix.append(table);
      }
      data.lookupFailurePrefix = prefix.toString();
    }
    StringBuilder description = new StringBuilder(data.lookupFailurePrefix);

    if (!data.lookupMeta.isEmpty()) {
      description.append(" for ");
      for (int i = 0; i < data.lookupMeta.size(); i++) {
        IValueMeta valueMeta = data.lookupMeta.getValueMeta(i);
        if (i > 0) {
          description.append(", ");
        }
        description.append(valueMeta.getName()).append('=');
        try {
          description.append(valueMeta.getString(lookupRow[i]));
        } catch (HopValueException e) {
          // a key we cannot render is no reason to fail the row: it is already being rejected
          description.append('?');
        }
      }
    }

    return description.toString();
  }

  private void initLookupMeta() throws HopException {
    // Count the number of values in the lookup as well as the metadata to send along with it.
    //
    data.lookupMeta = new RowMeta();
    final List<KeyField> keyFields = meta.getLookup().getKeyFields();

    for (int i = 0; i < keyFields.size(); i++) {
      if (data.keynrs[i] >= 0) {
        IValueMeta inputValueMeta = getInputRowMeta().getValueMeta(data.keynrs[i]);

        // Try to convert type if needed in a clone, we don't want to
        // change the type in the original row
        //
        IValueMeta value = ValueMetaFactory.cloneValueMeta(inputValueMeta, data.keytypes[i]);

        data.lookupMeta.addValueMeta(value);
      }
      if (data.keynrs2[i] >= 0) {
        IValueMeta inputValueMeta = getInputRowMeta().getValueMeta(data.keynrs2[i]);

        // Try to convert type if needed in a clone, we don't want to
        // change the type in the original row
        //
        IValueMeta value = ValueMetaFactory.cloneValueMeta(inputValueMeta, data.keytypes[i]);

        data.lookupMeta.addValueMeta(value);
      }
    }
  }

  private void initReturnMeta() {
    // We also want to know the metadata of the return values beforehand (null handling)
    data.returnMeta = new RowMeta();
    List<ReturnValue> returnValues = meta.getLookup().getReturnValues();

    data.returnValueTypes = new int[returnValues.size()];
    for (int i = 0; i < returnValues.size(); i++) {
      IValueMeta v = data.outputRowMeta.getValueMeta(getInputRowMeta().size() + i).clone();
      // Use resolved output type (may be inferred from the table when default type is empty)
      data.returnValueTypes[i] = v.getType();
      data.returnMeta.addValueMeta(v);
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      Lookup lookup = meta.getLookup();
      List<KeyField> keyFields = lookup.getKeyFields();
      List<ReturnValue> returnValues = lookup.getReturnValues();

      String[] keyField = new String[keyFields.size()];
      String[] keyCondition = new String[keyFields.size()];
      for (int i = 0; i < keyFields.size(); i++) {
        keyField[i] = keyFields.get(i).getTableField();
        keyCondition[i] = keyFields.get(i).getCondition();
      }
      String[] returnField = new String[returnValues.size()];
      String[] returnRename = new String[returnValues.size()];
      data.returnTrimTypes = new String[returnValues.size()];

      data.trimIndexes = new ArrayList<>();

      for (int i = 0; i < returnValues.size(); i++) {
        returnField[i] = returnValues.get(i).getTableField();
        returnRename[i] =
            Utils.isEmpty(returnValues.get(i).getNewName())
                ? null
                : returnValues.get(i).getNewName();
        data.returnTrimTypes[i] = returnValues.get(i).getTrimType();
      }

      data.db.setLookup(
          resolve(meta.getSchemaName()),
          resolve(meta.getTableName()),
          keyField,
          keyCondition,
          returnField,
          returnRename,
          lookup.getOrderByClause(),
          lookup.isFailingOnMultipleResults());

      // lookup the values!
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "DatabaseLookup.Log.CheckingRow")
                + getInputRowMeta().getString(r));
      }

      data.keynrs = new int[keyFields.size()];
      data.keynrs2 = new int[keyFields.size()];

      for (int i = 0; i < keyFields.size(); i++) {
        KeyField field = keyFields.get(i);
        data.keynrs[i] = getInputRowMeta().indexOfValue(field.getStreamField1());
        if (data.keynrs[i] < 0
            && // couldn't find field!
            !"IS NULL".equalsIgnoreCase(field.getCondition())
            && // No field needed!
            !"IS NOT NULL".equalsIgnoreCase(field.getCondition()) // No field needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "DatabaseLookup.ERROR0001.FieldRequired1.Exception")
                  + field.getStreamField1()
                  + BaseMessages.getString(
                      PKG, "DatabaseLookup.ERROR0001.FieldRequired2.Exception"));
        }
        data.keynrs2[i] = getInputRowMeta().indexOfValue(field.getStreamField2());
        if (data.keynrs2[i] < 0
            && // couldn't find field!
            "BETWEEN".equalsIgnoreCase(field.getCondition()) // 2 fields needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "DatabaseLookup.ERROR0001.FieldRequired3.Exception")
                  + field.getStreamField2()
                  + BaseMessages.getString(
                      PKG, "DatabaseLookup.ERROR0001.FieldRequired4.Exception"));
        }
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(PKG, "DatabaseLookup.Log.FieldHasIndex1")
                  + field.getStreamField1()
                  + BaseMessages.getString(PKG, "DatabaseLookup.Log.FieldHasIndex2")
                  + data.keynrs[i]);
        }
      }

      if (meta.isCached()) {
        data.cache = DefaultCache.newCache(data, meta.getCacheSize());
      }

      // Query table metadata once: key types + return type inference for empty default types
      determineFieldsTypesQueryingDb();

      // create the output metadata (pass table fields so empty return types can be inferred)
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(
          data.outputRowMeta,
          getTransformName(),
          new IRowMeta[] {data.dbRowMeta},
          null,
          this,
          metadataProvider);

      initNullIf();

      initLookupMeta();

      initReturnMeta();

      // See which return values need to be trimmed...
      //
      data.trimIndexes = new ArrayList();
      for (int i = 0; i < returnValues.size(); i++) {
        if (data.returnValueTypes[i] == IValueMeta.TYPE_STRING
            && ValueMetaBase.getTrimTypeByCode(data.returnTrimTypes[i])
                != IValueMeta.TRIM_TYPE_NONE) {
          data.trimIndexes.add(i);
        }
      }

      // If the user selected to load all data into the cache at startup, that's what we do now...
      //
      if (meta.isCached() && meta.isLoadingAllDataInCache()) {
        loadAllTableDataIntoTheCache();
      }
    }

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "DatabaseLookup.Log.GotRowFromPreviousTransform")
              + getInputRowMeta().getString(r));
    }

    try {
      // add new lookup values to the row
      Object[] outputRow = lookupValues(getInputRowMeta(), r);

      if (outputRow != null) {
        // copy row to output rowset(s)
        putRow(data.outputRowMeta, outputRow);

        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "DatabaseLookup.Log.WroteRowToNextTransform")
                  + data.outputRowMeta.getString(outputRow));
        }
        if (checkFeedback(getLinesRead()) && isBasic()) {
          logBasic("linenr " + getLinesRead());
        }
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(getInputRowMeta(), r, 1, e.getMessage(), null, "DBLOOKUPD001");
      } else {
        logError(
            BaseMessages.getString(PKG, "DatabaseLookup.ERROR003.UnexpectedErrorDuringProcessing")
                + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
    }

    return true;
  }

  private void loadAllTableDataIntoTheCache() throws HopException {
    DatabaseMeta dbMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

    Database db = getDatabase(dbMeta);
    connectDatabase(db);

    Lookup lookup = meta.getLookup();

    try {
      // We only want to get the used table fields...
      //
      String sql = "SELECT ";
      List<KeyField> keyFields = lookup.getKeyFields();

      for (int i = 0; i < keyFields.size(); i++) {
        KeyField keyField = keyFields.get(i);

        if (i > 0) {
          sql += ", ";
        }
        sql += dbMeta.quoteField(keyField.getTableField());
      }

      // Also grab the return field...
      //
      List<ReturnValue> returnValues = lookup.getReturnValues();
      for (ReturnValue returnValue : returnValues) {
        sql += ", " + dbMeta.quoteField(returnValue.getTableField());
      }
      // The schema/table
      //
      sql +=
          " FROM "
              + dbMeta.getQuotedSchemaTableCombination(
                  this, meta.getSchemaName(), meta.getTableName());

      // order by?
      if (StringUtils.isNotEmpty(lookup.getOrderByClause())) {
        sql += " ORDER BY " + lookup.getOrderByClause();
      }

      // Now that we have the SQL constructed, stream the rows into the cache: holding the whole
      // result as a list next to the cache would need twice the memory of the table.
      //
      ResultSet resultSet = db.openQuery(sql);
      try {
        if (data.allEquals) {
          putToDefaultCache(db, resultSet);
        } else {
          putToReadOnlyCache(db, resultSet);
        }
      } finally {
        db.closeQuery(resultSet);
      }
    } catch (Exception e) {
      throw new HopException(e);
    } finally {
      if (db != null) {
        db.disconnect();
      }
    }
  }

  private void putToDefaultCache(Database db, ResultSet resultSet) throws HopException {
    final int keysAmount = meta.getLookup().getKeyFields().size();
    final int valuesAmount = data.returnMeta.size();

    // All keys share one row meta: the cache only reads it.
    IRowMeta keyMeta = copyValueMetasFrom(db.getReturnRowMeta(), keysAmount).clone();

    // Copy the data into 2 parts: key and value...
    //
    Object[] row;
    while ((row = db.getRow(resultSet)) != null) {
      Object[] keyData = new Object[keysAmount];
      System.arraycopy(row, 0, keyData, 0, keysAmount);

      Object[] valueData = new Object[valuesAmount];
      System.arraycopy(row, keysAmount, valueData, 0, valuesAmount);
      trimCachedReturnValues(valueData);

      // Store the data...
      //
      data.cache.storeRowInCache(meta, keyMeta, keyData, valueData);
      incrementLinesInput();
    }
  }

  private IRowMeta copyValueMetasFrom(IRowMeta source, int n) {
    RowMeta result = new RowMeta();
    for (int i = 0; i < n; i++) {
      result.addValueMeta(source.getValueMeta(i));
    }
    return result;
  }

  private void putToReadOnlyCache(Database db, ResultSet resultSet) throws HopException {
    final int keysAmount = meta.getLookup().getKeyFields().size();
    final int valuesAmount = data.returnMeta.size();

    List<Object[]> keys = new ArrayList<>();
    List<Object[]> values = new ArrayList<>();

    // Copy the data into 2 parts: key and value...
    //
    Object[] row;
    while ((row = db.getRow(resultSet)) != null) {
      Object[] keyData = new Object[keysAmount];
      System.arraycopy(row, 0, keyData, 0, keysAmount);

      Object[] valueData = new Object[valuesAmount];
      System.arraycopy(row, keysAmount, valueData, 0, valuesAmount);
      trimCachedReturnValues(valueData);

      keys.add(keyData);
      values.add(valueData);
      incrementLinesInput();
    }
    if (keys.isEmpty()) {
      // nothing to index: keep the (empty) default cache
      return;
    }

    ReadAllCache.Builder cacheBuilder = new ReadAllCache.Builder(data, keys.size());

    // all keys have the same row meta,
    // it is useless to re-create it each time
    cacheBuilder.setKeysMeta(db.getReturnRowMeta().clone());
    for (int i = 0; i < keys.size(); i++) {
      cacheBuilder.add(keys.get(i), values.get(i));
    }
    data.cache = cacheBuilder.build();
  }

  /**
   * Trims a row going into the load-all cache, so a cache hit has nothing left to do. Only string
   * values are trimmed: the trim settings apply to string return values.
   */
  private void trimCachedReturnValues(Object[] values) throws HopValueException {
    for (int i : data.trimIndexes) {
      if (values[i] == null || values[i] instanceof String) {
        IValueMeta expected = data.returnMeta.getValueMeta(i);
        values[i] =
            expected.convertDataFromString(
                (String) values[i],
                expected,
                "",
                "",
                ValueMetaBase.getTrimTypeByCode(data.returnTrimTypes[i]));
      }
    }
  }

  /** Stop the running query */
  @Override
  public void stopRunning() throws HopException {

    if (data.db != null && !data.isCanceled) {
      synchronized (data.db) {
        data.db.cancelQuery();
      }
      data.isCanceled = true;
    }
  }

  @Override
  public boolean init() {

    if (super.init()) {

      if (Utils.isEmpty(meta.getConnection())) {
        logError(
            BaseMessages.getString(
                PKG, "DatabaseLookup.Init.ConnectionMissing", getTransformName()));
        return false;
      }
      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      data.db = getDatabase(databaseMeta);
      try {
        connectDatabase(data.db);

        Lookup lookup = meta.getLookup();
        List<KeyField> keyFields = lookup.getKeyFields();

        // See if all the lookup conditions are "equal"
        // This might speed up things in the case when we load all data in the cache
        //
        data.allEquals = true;
        data.hasDBCondition = false;
        data.conditions = new int[keyFields.size()];
        for (int i = 0; i < keyFields.size(); i++) {
          KeyField keyField = keyFields.get(i);
          data.conditions[i] =
              Const.indexOfString(keyField.getCondition(), DatabaseLookupMeta.conditionStrings);
          if (!("=".equals(keyField.getCondition())
              || "IS NULL".equalsIgnoreCase(keyField.getCondition()))) {
            data.allEquals = false;
          }
          if (data.conditions[i] == DatabaseLookupMeta.CONDITION_LIKE) {
            data.hasDBCondition = true;
          }
        }

        return true;
      } catch (Exception e) {
        logError(
            BaseMessages.getString(PKG, "DatabaseLookup.ERROR0004.UnexpectedErrorDuringInit") + e);
        if (data.db != null) {
          data.db.disconnect();
        }
      }
    }
    return false;
  }

  @Override
  public void dispose() {

    if (data.db != null) {
      data.db.disconnect();
    }

    // Recover memory immediately, allow in-memory data to be garbage collected
    //
    data.cache = null;
    data.db = null;

    super.dispose();
  }

  /*
   * this method is required in order to
   * provide ability for unit tests to
   * mock the main database instance for the transform
   * (@see org.apache.hop.pipeline.transforms.databaselookup.PDI5436Test)
   */
  Database getDatabase(DatabaseMeta meta) {
    return new Database(this, this, meta);
  }

  private void connectDatabase(Database database) throws HopDatabaseException {
    database.connect();

    database.setCommit(100); // we never get a commit, but it just turns off auto-commit.

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "DatabaseLookup.Log.ConnectedToDatabase"));
    }
  }
}
