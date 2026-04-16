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

package org.apache.hop.pipeline.transforms.valuemapper;

import java.util.HashMap;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Convert Values in a certain fields to other values */
public class ValueMapper extends BaseTransform<ValueMapperMeta, ValueMapperData> {
  private static final Class<?> PKG = ValueMapperMeta.class;
  public static final String CONST_STRING = "String";

  private boolean nonMatchActivated = false;

  public ValueMapper(
      TransformMeta transformMeta,
      ValueMapperMeta meta,
      ValueMapperData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    // Get one row from one of the rowsets...
    //
    Object[] r = getRow();
    if (r == null) { // means: no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.previousMeta = getInputRowMeta().clone();
      data.outputMeta = data.previousMeta.clone();
      meta.getFields(data.outputMeta, getTransformName(), null, null, this, metadataProvider);

      data.keynr = data.previousMeta.indexOfValue(meta.getFieldToUse());
      if (data.keynr < 0) {
        String message =
            BaseMessages.getString(
                PKG,
                "ValueMapper.RuntimeError.FieldToUseNotFound.VALUEMAPPER0001",
                meta.getFieldToUse(),
                Const.CR,
                getInputRowMeta().getString(r));
        logError(message);
        setErrors(1);
        stopAll();
        return false;
      }

      data.sourceValueMeta = getInputRowMeta().getValueMeta(data.keynr);
      setTargetMetaType();
      builMapValues();

      if (Utils.isEmpty(meta.getTargetField())) {
        data.outputValueMeta = data.outputMeta.getValueMeta(data.keynr); // Same field

      } else {
        data.outputValueMeta = data.outputMeta.searchValueMeta(meta.getTargetField()); // new field
      }
    }

    Object sourceValue = r[data.keynr]; // could be any storage type

    // Use only normal storage type in the HashMap
    Object sourceData = data.sourceValueMeta.convertToNormalStorageType(sourceValue);
    Object target = null;
    boolean mapped = false;

    if (data.emptySourceMappingDefined && (r[data.keynr] == null || sourceData == null)) {
      target = data.emptyFieldValue;
      mapped = true;
    } else if (sourceData != null) {
      if (data.mapValues.containsKey(sourceData)) {
        target = data.mapValues.get(sourceData);
        mapped = true;
      } else if (nonMatchActivated) {
        target = data.nonMatchDefault;
        mapped = true;
      }
    }

    if (!Utils.isEmpty(meta.getTargetField())) {
      // room for the target
      r = RowDataUtil.resizeArray(r, data.outputMeta.size());
      // Did we find anything to map to?
      r[data.outputMeta.size() - 1] = target;
    } else {
      if (mapped) {
        r[data.keynr] = target;
      } else {
        // Convert to normal storage type.
        // Otherwise we're going to be mixing storage types.
        //
        if (data.sourceValueMeta.isStorageBinaryString()) {
          Object normal = data.sourceValueMeta.convertToNormalStorageType(r[data.keynr]);
          r[data.keynr] = normal;
        }
      }
    }
    putRow(data.outputMeta, r);

    return true;
  }

  /**
   * Convert a String key to the target meta's NORMAL storage object using srcMeta as the source
   * type.
   */
  private Object keyFromString(IValueMeta tgtMeta, IValueMeta srcMeta, String key)
      throws HopValueException {
    Object v = tgtMeta.convertData(srcMeta, key);
    return tgtMeta.convertToNormalStorageType(v);
  }

  private String typeName(IValueMeta vm) {
    return vm == null ? "<unknown>" : vm.toStringMeta();
  }

  /** Build the value map, default-on-nonmatch, and empty/null mapping. */
  private void builMapValues() throws HopException {

    data.emptySourceMappingDefined = false;
    IValueMeta stringValueMeta = new ValueMetaString(CONST_STRING);
    // --- Default for non-match --------------
    //
    try {
      if (!Utils.isEmpty(meta.getNonMatchDefault())) {
        nonMatchActivated = true;
        String nonMatchStr = resolve(meta.getNonMatchDefault());
        data.nonMatchDefault = keyFromString(data.targetValueMeta, stringValueMeta, nonMatchStr);
      }
    } catch (HopValueException e) {
      String msg =
          String.format(
              "Cannot convert the \"Default upon non-matching\" value [%s] to target type [%s].",
              resolve(meta.getNonMatchDefault()), typeName(data.targetValueMeta));
      throw new HopValueException(msg, e);
    }

    // --- HashMap --------------
    // Add all source to target mappings in here...
    for (Values v : meta.getValues()) {
      String src = v.getSource();
      String tgt = this.resolve(v.getTarget());

      Object srcValue;
      try {
        srcValue = keyFromString(data.sourceValueMeta, stringValueMeta, src);
      } catch (HopValueException ce) {
        String msg =
            String.format(
                "Mapping entries: cannot convert source [%s] to source type [%s].",
                src, typeName(data.sourceValueMeta));
        throw new HopValueException(msg, ce);
      }
      Object tgtValue;
      if (v.isEmptyStringEqualsNull() && Utils.isEmpty(tgt)) {
        tgtValue = null;
      } else {
        try {
          tgtValue = keyFromString(data.targetValueMeta, stringValueMeta, tgt);
        } catch (HopValueException ce) {
          String msg =
              String.format(
                  "Mapping entries: cannot convert target [%s] to target type [%s].",
                  src, typeName(data.sourceValueMeta));
          throw new HopValueException(msg, ce);
        }
      }

      if (srcValue != null) {
        data.mapValues.put(srcValue, tgtValue);
      }
    }

    // Null handling:
    // 0 or 1 empty mapping is allowed, not 2 or more.
    //
    for (Values v : meta.getValues()) {
      if (Utils.isEmpty(v.getSource())) {
        if (!data.emptySourceMappingDefined) {
          String emptyFieldString = resolve(v.getTarget());
          if (v.isEmptyStringEqualsNull() && Utils.isEmpty(emptyFieldString)) {
            data.emptyFieldValue = null;
          } else {
            data.emptyFieldValue =
                keyFromString(data.targetValueMeta, stringValueMeta, emptyFieldString);
          }
          data.emptySourceMappingDefined = true;
        } else {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ValueMapper.RuntimeError.OnlyOneEmptyMappingAllowed.VALUEMAPPER0004"));
        }
      }
    }
  }

  /**
   * Resolve and set the target output meta type (user-selected, or String when unspecified to match
   * {@link ValueMapperMeta#getFields}).
   */
  private void setTargetMetaType() {
    try {
      int targetValueMetaId;
      String targetValueMetaName;

      if (!Utils.isEmpty(meta.getTargetType())) {
        targetValueMetaName = meta.getTargetType();
        targetValueMetaId = ValueMetaFactory.getIdForValueMeta(targetValueMetaName);
        if (targetValueMetaId == IValueMeta.TYPE_NONE) {
          targetValueMetaId = IValueMeta.TYPE_STRING;
          targetValueMetaName = CONST_STRING;
        }
      } else {
        targetValueMetaId = IValueMeta.TYPE_STRING;
        targetValueMetaName = CONST_STRING;
      }

      data.targetValueMeta =
          ValueMetaFactory.createValueMeta(targetValueMetaName, targetValueMetaId);
    } catch (HopException e) {
      data.targetValueMeta = new ValueMetaString(CONST_STRING);
    }
  }

  @Override
  public void dispose() {
    super.dispose();
  }

  @Override
  public boolean init() {

    if (super.init()) {
      data.mapValues = new HashMap<>();

      return true;
    }
    return false;
  }
}
