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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Arrays;
import java.util.Set;

/** Filters input rows base on conditions. */
public class SwitchCase extends BaseTransform<SwitchCaseMeta, SwitchCaseData>
    implements ITransform<SwitchCaseMeta, SwitchCaseData> {
  private static final Class<?> PKG = SwitchCaseMeta.class; // For Translator

  public SwitchCase(
      TransformMeta transformMeta,
      SwitchCaseMeta meta,
      SwitchCaseData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get next usable row from input rowset(s)!
    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      // map input to output streams
      createOutputValueMapping();
    }

    // We already know the target values, but we need to make sure that the input data type is the
    // same as the specified
    // one.
    // Perhaps there is some conversion needed.
    //
    Object lookupData = data.valueMeta.convertData(data.inputValueMeta, r[data.fieldIndex]);

    // could not use byte[] as key in Maps, so we need to convert it to his specific hashCode for
    // comparisons
    lookupData = prepareObjectType(lookupData);

    // Determine the output set of rowset to use...
    Set<IRowSet> rowSetSet =
        (data.valueMeta.isNull(lookupData)) ? data.nullRowSetSet : data.outputMap.get(lookupData);

    // If the rowset is still not found (unspecified key value, we drop down to the default option
    // For now: send it to the default transform...
    if (rowSetSet == null) {
      rowSetSet = data.defaultRowSetSet;
    }

    for (IRowSet rowSet : rowSetSet) {
      putRowTo(data.outputRowMeta, r, rowSet);
    }

    if (checkFeedback(getLinesRead())) {
      if (log.isBasic()) {
        logBasic(BaseMessages.getString(PKG, "SwitchCase.Log.LineNumber") + getLinesRead());
      }
    }

    return true;
  }

  /** @see ITransform#init() */
  @Override
  public boolean init() {

    if (!super.init()) {
      return false;
    }
    data.outputMap = meta.isUsingContains() ? new ContainsKeyToRowSetMap() : new KeyToRowSetMap();

    if (Utils.isEmpty(meta.getFieldName())) {
      logError(BaseMessages.getString(PKG, "SwitchCase.Log.NoFieldSpecifiedToSwitchWith"));
      return false;
    }

    try {
      int type = ValueMetaFactory.getIdForValueMeta(meta.getCaseValueType());
      data.valueMeta = ValueMetaFactory.createValueMeta(meta.getFieldName(), type);
      data.valueMeta.setConversionMask(meta.getCaseValueFormat());
      data.valueMeta.setGroupingSymbol(meta.getCaseValueGroup());
      data.valueMeta.setDecimalSymbol(meta.getCaseValueDecimal());
      data.stringValueMeta =
          ValueMetaFactory.cloneValueMeta(data.valueMeta, IValueMeta.TYPE_STRING);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "SwitchCase.Log.UnexpectedError", e));
    }

    return true;
  }

  /**
   * This will prepare transform for execution:
   *
   * <ol>
   *   <li>will copy input row meta info, fields info, etc. transform related info
   *   <li>will get transform IO meta info and discover target streams for target output transforms
   *   <li>for every target output find output rowset and expected value.
   *   <li>for every discovered output rowset put it as a key-value: 'expected value'-'output
   *       rowSet'. If expected value is null - put output rowset to special 'null set' (avoid usage
   *       of null as a map keys)
   *   <li>Discover default row set. We expect only one default rowset, even if technically can have
   *       many. *
   * </ol>
   *
   * @throws HopException if something goes wrong during transform preparation.
   */
  void createOutputValueMapping() throws HopException {
    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields(getInputRowMeta(), getTransformName(), null, null, this, metadataProvider);

    data.fieldIndex = getInputRowMeta().indexOfValue(meta.getFieldName());
    if (data.fieldIndex < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "SwitchCase.Exception.UnableToFindFieldName", meta.getFieldName()));
    }

    data.inputValueMeta = getInputRowMeta().getValueMeta(data.fieldIndex);

    try {
      for (SwitchCaseTarget target : meta.getCaseTargets()) {
        if (StringUtils.isEmpty(target.getCaseTargetTransformName())) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "SwitchCase.Log.NoTargetTransformSpecifiedForValue", target.getCaseValue()));
        }

        IRowSet rowSet = findOutputRowSet(target.getCaseTargetTransformName());
        if (rowSet == null) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "SwitchCase.Log.UnableToFindTargetRowSetForTransform",
                  target.getCaseTargetTransformName()));
        }

        try {
          Object value =
              data.valueMeta.convertDataFromString(
                  target.getCaseValue(),
                  data.stringValueMeta,
                  null,
                  null,
                  IValueMeta.TRIM_TYPE_NONE);

          // If we have a value and a rowset, we can store the combination in the map
          //
          if (data.valueMeta.isNull(value)) {
            data.nullRowSetSet.add(rowSet);
          } else {
            // could not use byte[] as key in Maps, so we need to convert it to his specific
            // hashCode for future
            // comparisons
            value = prepareObjectType(value);
            data.outputMap.put(value, rowSet);
          }
        } catch (Exception e) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "SwitchCase.Log.UnableToConvertValue", target.getCaseValue()),
              e);
        }
      }

      if (StringUtils.isNotEmpty(meta.getDefaultTargetTransformName())) {
        IRowSet rowSet = findOutputRowSet(meta.getDefaultTargetTransformName());
        if (rowSet != null) {
          data.defaultRowSetSet.add(rowSet);
          if (data.nullRowSetSet.isEmpty()) {
            data.nullRowSetSet.add(rowSet);
          }
        }
      }
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  protected static Object prepareObjectType(Object o) {
    return (o instanceof byte[]) ? Arrays.hashCode((byte[]) o) : o;
  }
}
