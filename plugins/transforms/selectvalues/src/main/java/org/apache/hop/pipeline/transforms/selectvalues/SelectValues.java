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

package org.apache.hop.pipeline.transforms.selectvalues;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopConversionException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Select, re-order, remove or change the meta-data of the fields in the inputstreams. */
public class SelectValues extends BaseTransform<SelectValuesMeta, SelectValuesData> {

  private static final Class<?> PKG = SelectValuesMeta.class;
  public static final String CONST_SELECT_VALUES_LOG_COULD_NOT_FIND_FIELD =
      "SelectValues.Log.CouldNotFindField";

  public SelectValues(
      TransformMeta transformMeta,
      SelectValuesMeta meta,
      SelectValuesData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Only select the values that are still needed...
   *
   * <p>Put the values in the right order...
   *
   * <p>Change the meta-data information if needed...
   *
   * <p>
   *
   * @param rowMeta The row to manipulate
   * @param rowData the row data to manipulate
   * @return true if everything went well, false if we need to stop because of an error!
   */
  private synchronized Object[] selectValues(IRowMeta rowMeta, Object[] rowData)
      throws HopValueException {
    if (data.firstselect) {
      data.firstselect = false;

      // We need to create a new meta-data row to drive the output
      // We also want to know the indexes of the selected fields in the source row.
      //
      data.fieldnrs = new int[meta.getSelectOption().getSelectFields().size()];
      for (int i = 0; i < data.fieldnrs.length; i++) {
        data.fieldnrs[i] =
            rowMeta.indexOfValue(meta.getSelectOption().getSelectFields().get(i).getName());
        if (data.fieldnrs[i] < 0) {
          logError(
              BaseMessages.getString(
                  PKG,
                  CONST_SELECT_VALUES_LOG_COULD_NOT_FIND_FIELD,
                  meta.getSelectOption().getSelectFields().get(i).getName()));
          setErrors(1);
          stopAll();
          return null;
        }
      }

      // Check for doubles in the selected fields... AFTER renaming!!
      //
      int[] cnt = new int[meta.getSelectOption().getSelectFields().size()];
      for (int i = 0; i < meta.getSelectOption().getSelectFields().size(); i++) {
        cnt[i] = 0;
        for (int j = 0; j < meta.getSelectOption().getSelectFields().size(); j++) {
          String one =
              Const.NVL(
                  meta.getSelectOption().getSelectFields().get(i).getRename(),
                  meta.getSelectOption().getSelectFields().get(i).getName());
          String two =
              Const.NVL(
                  meta.getSelectOption().getSelectFields().get(j).getRename(),
                  meta.getSelectOption().getSelectFields().get(j).getName());
          if (one.equals(two)) {
            cnt[i]++;
          }

          if (cnt[i] > 1) {
            logError(
                BaseMessages.getString(
                    PKG, "SelectValues.Log.FieldCouldNotSpecifiedMoreThanTwice", one));
            setErrors(1);
            stopAll();
            return null;
          }
        }
      }

      // See if we need to include (and sort) the non-specified fields as well...
      //
      if (meta.getSelectOption().isSelectingAndSortingUnspecifiedFields()) {
        // Select the unspecified fields.
        // Sort the fields
        // Add them after the specified fields...
        //
        List<String> extra = new ArrayList<>();
        ArrayList<Integer> unspecifiedKeyNrs = new ArrayList<>();
        for (int i = 0; i < rowMeta.size(); i++) {
          String fieldName = rowMeta.getValueMeta(i).getName();
          if (Const.indexOfString(fieldName, meta.getSelectName()) < 0) {
            extra.add(fieldName);
          }
        }
        Collections.sort(extra);
        for (String fieldName : extra) {
          int index = rowMeta.indexOfValue(fieldName);
          unspecifiedKeyNrs.add(index);
        }

        // Create the extra field list...
        //
        data.extraFieldnrs = new int[unspecifiedKeyNrs.size()];
        for (int i = 0; i < data.extraFieldnrs.length; i++) {
          data.extraFieldnrs[i] = unspecifiedKeyNrs.get(i);
        }
      } else {
        data.extraFieldnrs = new int[] {};
      }
    }

    // Create a new output row
    Object[] outputData = new Object[data.selectRowMeta.size()];
    int outputIndex = 0;

    // Get the field values
    //
    for (int idx : data.fieldnrs) {
      // Normally this can't happen, except when streams are mixed with different
      // number of fields.
      //
      if (idx < rowMeta.size()) {
        IValueMeta valueMeta = rowMeta.getValueMeta(idx);

        // TODO: Clone might be a 'bit' expensive as it is only needed in case you want to copy a
        // single field to 2 or
        // more target fields.
        // And even then it is only required for the last n-1 target fields.
        // Perhaps we can consider the requirements for cloning at init(), store it in a boolean[]
        // and just consider
        // this at runtime
        //
        outputData[outputIndex++] = valueMeta.cloneValueData(rowData[idx]);
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "SelectValues.Log.MixingStreamWithDifferentFields"));
        }
      }
    }

    // Do we need to drag the rest of the row also in there?
    //
    for (int idx : data.extraFieldnrs) {
      outputData[outputIndex++] = rowData[idx]; // always just a copy, can't be specified twice.
    }

    return outputData;
  }

  /**
   * Remove the values that are no longer needed.
   *
   * <p>
   *
   * @param rowMeta The row metadata to change
   * @param rowData the row data to change
   * @return true if everything went well, false if we need to stop because of an error!
   */
  private synchronized Object[] removeValues(IRowMeta rowMeta, Object[] rowData) {
    if (data.firstdeselect) {
      data.firstdeselect = false;

      var deleteNames = meta.getSelectOption().getDeleteName();
      data.removenrs = new int[deleteNames.size()];
      for (int i = 0; i < data.removenrs.length; i++) {
        var deleteName = deleteNames.get(i);
        data.removenrs[i] = rowMeta.indexOfValue(deleteName.getName());
        if (data.removenrs[i] < 0) {
          logError(
              BaseMessages.getString(
                  PKG, CONST_SELECT_VALUES_LOG_COULD_NOT_FIND_FIELD, deleteName));
          setErrors(1);
          stopAll();
          return null;
        }
      }

      // Check for doubles in the selected fields...
      int[] cnt = new int[deleteNames.size()];
      for (int i = 0; i < deleteNames.size(); i++) {
        cnt[i] = 0;
        for (int j = 0; j < meta.getSelectOption().getDeleteName().size(); j++) {
          if (meta.getSelectOption()
              .getDeleteName()
              .get(i)
              .equals(meta.getSelectOption().getDeleteName().get(j))) {
            cnt[i]++;
          }

          if (cnt[i] > 1) {
            logError(
                BaseMessages.getString(
                    PKG,
                    "SelectValues.Log.FieldCouldNotSpecifiedMoreThanTwice2",
                    meta.getSelectOption().getDeleteName().get(i)));
            setErrors(1);
            stopAll();
            return null;
          }
        }
      }

      // Sort removenrs descending. So that we can delete in ascending order...
      Arrays.sort(data.removenrs);
    }

    /*
     * Remove the field values Take into account that field indexes change once you remove them!!! Therefore removenrs
     * is sorted in reverse on index...
     */
    return RowDataUtil.removeItems(rowData, data.removenrs);
  }

  /**
   * Change the meta-data of certain fields.
   *
   * <p>This, we can do VERY fast.
   *
   * <p>
   *
   * @param rowMeta The row metadata to change
   * @param rowData the row data to change
   * @return the altered RowData array
   * @throws HopValueException
   */
  @VisibleForTesting
  synchronized Object[] metadataValues(IRowMeta rowMeta, Object[] rowData) throws HopException {
    if (data.firstmetadata) {
      data.firstmetadata = false;

      data.metanrs = new int[meta.getSelectOption().getMeta().size()];
      for (int i = 0; i < data.metanrs.length; i++) {
        data.metanrs[i] = rowMeta.indexOfValue(meta.getSelectOption().getMeta().get(i).getName());
        if (data.metanrs[i] < 0) {
          logError(
              BaseMessages.getString(
                  PKG,
                  CONST_SELECT_VALUES_LOG_COULD_NOT_FIND_FIELD,
                  meta.getSelectOption().getMeta().get(i).getName()));
          setErrors(1);
          stopAll();
          return null;
        }
      }

      // Check for doubles in the selected fields...
      int[] cnt = new int[meta.getSelectOption().getMeta().size()];
      for (int i = 0; i < meta.getSelectOption().getMeta().size(); i++) {
        cnt[i] = 0;
        for (int j = 0; j < meta.getSelectOption().getMeta().size(); j++) {
          if (meta.getSelectOption()
              .getMeta()
              .get(i)
              .getName()
              .equals(meta.getSelectOption().getMeta().get(j).getName())) {
            cnt[i]++;
          }

          if (cnt[i] > 1) {
            logError(
                BaseMessages.getString(
                    PKG,
                    "SelectValues.Log.FieldCouldNotSpecifiedMoreThanTwice2",
                    meta.getSelectOption().getMeta().get(i).getName()));
            setErrors(1);
            stopAll();
            return null;
          }
        }
      }

      // Also apply the metadata on the row meta to allow us to convert the data correctly, with the
      // correct mask.
      //
      for (int i = 0; i < data.metanrs.length; i++) {
        SelectMetadataChange change = meta.getSelectOption().getMeta().get(i);
        IValueMeta valueMeta = rowMeta.getValueMeta(data.metanrs[i]);
        if (!Utils.isEmpty(change.getConversionMask())) {
          valueMeta.setConversionMask(change.getConversionMask());
        }

        valueMeta.setDateFormatLenient(change.isDateFormatLenient());
        valueMeta.setDateFormatLocale(EnvUtil.createLocale(change.getDateFormatLocale()));
        valueMeta.setDateFormatTimeZone(EnvUtil.createTimeZone(change.getDateFormatTimeZone()));
        valueMeta.setLenientStringToNumber(change.isLenientStringToNumber());

        if (!Utils.isEmpty(change.getEncoding())) {
          valueMeta.setStringEncoding(change.getEncoding());
        }
        if (!Utils.isEmpty(change.getDecimalSymbol())) {
          valueMeta.setDecimalSymbol(change.getDecimalSymbol());
        }
        if (!Utils.isEmpty(change.getGroupingSymbol())) {
          valueMeta.setGroupingSymbol(change.getGroupingSymbol());
        }
        if (!Utils.isEmpty(change.getCurrencySymbol())) {
          valueMeta.setCurrencySymbol(change.getCurrencySymbol());
        }
      }
    }

    //
    // Change the data too
    //
    for (int i = 0; i < data.metanrs.length; i++) {
      int index = data.metanrs[i];
      IValueMeta fromMeta = rowMeta.getValueMeta(index);
      IValueMeta toMeta = data.metadataRowMeta.getValueMeta(index);

      // If we need to change from BINARY_STRING storage type to NORMAL...
      //
      try {
        if (fromMeta.isStorageBinaryString()
            && meta.getSelectOption().getMeta().get(i).getStorageType()
                == ValueMetaFactory.getValueMetaName(IValueMeta.STORAGE_TYPE_NORMAL)) {
          rowData[index] = fromMeta.convertBinaryStringToNativeType((byte[]) rowData[index]);
        }
        if (meta.getSelectOption().getMeta().get(i).getType()
                != ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NONE)
            && fromMeta.getType() != toMeta.getType()) {
          rowData[index] = toMeta.convertData(fromMeta, rowData[index]);
        }
      } catch (HopValueException e) {
        throw new HopConversionException(
            e.getMessage(),
            Collections.singletonList(e),
            Collections.singletonList(toMeta),
            rowData);
      }
    }

    return rowData;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] rowData = getRow(); // get row from rowset, wait for our turn, indicate busy!
    if (rowData == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    Object[] rowCopy = null;
    if (getTransformMeta().isDoingErrorHandling()) {
      rowCopy = getInputRowMeta().cloneRow(rowData);
    }

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "SelectValues.Log.GotRowFromPreviousTransform")
              + getInputRowMeta().getString(rowData));
    }

    if (first) {
      first = false;

      data.selectRowMeta = getInputRowMeta().clone();
      meta.getSelectFields(data.selectRowMeta, getTransformName());
      data.deselectRowMeta = data.selectRowMeta.clone();
      meta.getDeleteFields(data.deselectRowMeta);
      data.metadataRowMeta = data.deselectRowMeta.clone();
      meta.getMetadataFields(data.metadataRowMeta, getTransformName(), this);
    }

    try {
      Object[] outputData = rowData;

      if (data.select) {
        outputData = selectValues(getInputRowMeta(), outputData);
      }
      if (data.deselect) {
        outputData = removeValues(data.selectRowMeta, outputData);
      }
      if (data.metadata) {
        outputData = metadataValues(data.deselectRowMeta, outputData);
      }

      if (outputData == null) {
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      // Send the row on its way
      //
      putRow(data.metadataRowMeta, outputData);
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "SelectValues.Log.WroteRowToNextTransform")
                + data.metadataRowMeta.getString(outputData));
      }

    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        String field;
        if (e instanceof HopConversionException hopConversionException) {
          List<IValueMeta> fields = hopConversionException.getFields();
          field = fields.isEmpty() ? null : fields.get(0).getName();
        } else {
          field = null;
        }
        putError(getInputRowMeta(), rowCopy, 1, e.getMessage(), field, "SELECT001");
      } else {
        throw e;
      }
    }

    if (checkFeedback(getLinesRead())) {
      logBasic(BaseMessages.getString(PKG, "SelectValues.Log.LineNumber") + getLinesRead());
    }

    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      data.firstselect = true;
      data.firstdeselect = true;
      data.firstmetadata = true;

      data.select = false;
      data.deselect = false;
      data.metadata = false;

      if (!Utils.isEmpty(meta.getSelectOption().getSelectFields())) {
        data.select = true;
      }
      if (!Utils.isEmpty(meta.getSelectOption().getDeleteName())) {
        data.deselect = true;
      }
      if (!Utils.isEmpty(meta.getSelectOption().getMeta())) {
        data.metadata = true;
      }

      boolean atLeastOne = data.select || data.deselect || data.metadata;
      if (!atLeastOne) {
        setErrors(1);
        logError(BaseMessages.getString(PKG, "SelectValues.Log.InputShouldContainData"));
      }

      return atLeastOne; // One of those three has to work!
    } else {
      return false;
    }
  }
}
