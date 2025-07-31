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

package org.apache.hop.pipeline.transforms.memgroupby;

import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.CountAll;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.CountAny;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.CountDistinct;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.Percentile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByData.HashEntry;

/** Groups information based on aggregation rules. (sum, count, ...) */
public class MemoryGroupBy extends BaseTransform<MemoryGroupByMeta, MemoryGroupByData> {
  private static final Class<?> PKG = MemoryGroupByMeta.class;

  private boolean allNullsAreZero = false;
  private boolean minNullIsValued = false;

  public MemoryGroupBy(
      TransformMeta transformMeta,
      MemoryGroupByMeta meta,
      MemoryGroupByData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row!

    if (first) {
      if ((r == null) && (!meta.isAlwaysGivingBackOneRow())) {
        setOutputDone();
        return false;
      }

      String val = getVariable(Const.HOP_AGGREGATION_ALL_NULLS_ARE_ZERO, "N");
      allNullsAreZero = ValueMetaBase.convertStringToBoolean(val);
      val = getVariable(Const.HOP_AGGREGATION_MIN_NULL_IS_VALUED, "N");
      minNullIsValued = ValueMetaBase.convertStringToBoolean(val);

      // What is the output looking like?
      //
      data.inputRowMeta = getInputRowMeta();

      // In case we have 0 input rows, we still want to send out a single row aggregate
      // However, the problem then is that we don't know the layout from receiving it from the
      // previous transform over the row set.
      // So we need to calculated based on the metadata...
      //
      if (data.inputRowMeta == null) {
        data.inputRowMeta = getPipelineMeta().getPrevTransformFields(this, getTransformMeta());
      }

      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Do all the work we can beforehand
      // Calculate indexes, loop up fields, etc.
      //
      data.subjectnrs = new int[meta.getAggregates().size()];
      data.groupnrs = new int[meta.getGroups().size()];

      // If the transform does not receive any rows, we can not lookup field position indexes
      if (r != null) {
        for (int i = 0; i < meta.getAggregates().size(); i++) {
          GAggregate aggregate = meta.getAggregates().get(i);
          if (aggregate.getType() == CountAny) {
            data.subjectnrs[i] = 0;
          } else {
            data.subjectnrs[i] = data.inputRowMeta.indexOfValue(aggregate.getSubject());
          }
          if (data.subjectnrs[i] < 0) {
            logError(
                BaseMessages.getString(
                    PKG,
                    "MemoryGroupBy.Log.AggregateSubjectFieldCouldNotFound",
                    aggregate.getSubject()));
            setErrors(1);
            stopAll();
            return false;
          }
        }

        for (int i = 0; i < meta.getGroups().size(); i++) {
          String groupField = meta.getGroups().get(i).getField();
          data.groupnrs[i] = data.inputRowMeta.indexOfValue(groupField);
          if (data.groupnrs[i] < 0) {
            logError(
                BaseMessages.getString(
                    PKG, "MemoryGroupBy.Log.GroupFieldCouldNotFound", groupField));
            setErrors(1);
            stopAll();
            return false;
          }
        }
      }

      // Create a metadata value for the counter Integers
      //
      data.valueMetaInteger = new ValueMetaInteger("count");
      data.valueMetaNumber = new ValueMetaNumber("sum");

      // Initialize the group metadata
      //
      initGroupMeta(data.inputRowMeta);
    }

    if (first) {
      // Only calculate data.aggMeta here, not for every new aggregate.
      //
      newAggregate(r, null);

      // for speed: groupMeta+aggMeta
      //
      data.groupAggMeta = new RowMeta();
      data.groupAggMeta.addRowMeta(data.groupMeta);
      data.groupAggMeta.addRowMeta(data.aggMeta);
    }

    // Here is where we start to do the real work...
    //
    if (r == null) { // no more input to be expected... (or none received in the first place)
      handleLastOfGroup();

      setOutputDone();
      return false;
    }

    if (first || data.newBatch) {
      first = false;
      data.newBatch = false;
    }

    addToAggregate(r);

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "MemoryGroupBy.LineNumber") + getLinesRead());
    }

    return true;
  }

  private void handleLastOfGroup() throws HopException {
    // Dump the content of the map...
    //
    for (HashEntry entry : data.map.keySet()) {
      Aggregate aggregate = data.map.get(entry);
      Object[] aggregateResult = getAggregateResult(aggregate);

      Object[] outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int index = 0;
      for (int i = 0; i < data.groupMeta.size(); i++) {
        outputRowData[index++] =
            data.groupMeta.getValueMeta(i).convertToNormalStorageType(entry.getGroupData()[i]);
      }
      for (int i = 0; i < data.aggMeta.size(); i++) {
        outputRowData[index++] =
            data.aggMeta.getValueMeta(i).convertToNormalStorageType(aggregateResult[i]);
      }
      putRow(data.outputRowMeta, outputRowData);
    }

    // What if we always need to give back one row?
    // This means we give back 0 for count all, count distinct, null for everything else
    //
    if (data.map.isEmpty() && meta.isAlwaysGivingBackOneRow()) {
      Object[] outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int index = 0;
      for (int i = 0; i < data.groupMeta.size(); i++) {
        outputRowData[index++] = null;
      }
      for (int i = 0; i < data.aggMeta.size(); i++) {
        GAggregate aggregate = meta.getAggregates().get(i);
        if (aggregate.getType() == CountAll
            || aggregate.getType() == CountAny
            || aggregate.getType() == CountDistinct) {
          outputRowData[index++] = 0L;
        } else {
          outputRowData[index++] = null;
        }
      }
      putRow(data.outputRowMeta, outputRowData);
    }
  }

  /**
   * Used for junits in MemoryGroupByAggregationNullsTest
   *
   * @param r
   * @throws HopException
   */
  void addToAggregate(Object[] r) throws HopException {

    Object[] groupData = new Object[data.groupMeta.size()];
    for (int i = 0; i < data.groupnrs.length; i++) {
      groupData[i] = r[data.groupnrs[i]];
    }
    HashEntry entry = data.getHashEntry(groupData);

    Aggregate aggregate = data.map.get(entry);
    if (aggregate == null) {
      // Create a new value...
      //
      aggregate = new Aggregate();
      newAggregate(r, aggregate);

      // Store it in the map!
      //
      data.map.put(entry, aggregate);
    }

    for (int i = 0; i < data.subjectnrs.length; i++) {
      Object subj = r[data.subjectnrs[i]];
      IValueMeta subjMeta = data.inputRowMeta.getValueMeta(data.subjectnrs[i]);
      Object value = aggregate.agg[i];
      IValueMeta valueMeta = data.aggMeta.getValueMeta(i);
      GAggregate agg = meta.getAggregates().get(i);

      switch (agg.getType()) {
        case Sum:
          aggregate.agg[i] = ValueDataUtil.sum(valueMeta, value, subjMeta, subj);
          break;
        case Average:
          if (!subjMeta.isNull(subj)) {
            aggregate.agg[i] = ValueDataUtil.sum(valueMeta, value, subjMeta, subj);
            aggregate.counts[i]++;
          }
          break;
        case Median, Percentile:
          if (!subjMeta.isNull(subj)) {
            ((List<Double>) aggregate.agg[i]).add(subjMeta.getNumber(subj));
          }
          break;
        case StandardDeviation:
          if (aggregate.mean == null) {
            aggregate.mean = new double[meta.getAggregates().size()];
          }
          aggregate.counts[i]++;
          double n = aggregate.counts[i];
          double x = subjMeta.getNumber(subj);
          // for standard deviation null is exact 0
          double sum = value == null ? Double.valueOf(0) : (Double) value;
          double mean = aggregate.mean[i];

          double delta = x - mean;
          mean = mean + (delta / n);
          sum = sum + delta * (x - mean);

          aggregate.mean[i] = mean;
          aggregate.agg[i] = sum;
          break;
        case CountDistinct:
          if (aggregate.distinctObjs == null) {
            aggregate.distinctObjs = new Set[meta.getAggregates().size()];
          }
          if (aggregate.distinctObjs[i] == null) {
            aggregate.distinctObjs[i] = new TreeSet<>();
          }
          if (!subjMeta.isNull(subj)) {
            Object obj = subjMeta.convertToNormalStorageType(subj);
            // byte [] is not Comparable and can not be added to TreeSet.
            // For our case it can be binary array. It was typed as String.
            // So it can be processing (comparing and displaying) correctly as String
            if (obj instanceof byte[] bytes) {
              obj = new String(bytes);
            }
            if (!aggregate.distinctObjs[i].contains(obj)) {
              aggregate.distinctObjs[i].add(obj);
            }
          }
          aggregate.counts[i] = aggregate.distinctObjs[i].size();
          break;
        case CountAll:
          if (!subjMeta.isNull(subj)) {
            aggregate.counts[i]++;
          }
          break;
        case CountAny:
          aggregate.counts[i]++;
          break;
        case Minimum:
          boolean subjIsNull = subjMeta.isNull(subj);
          boolean valueIsNull = valueMeta.isNull(value);
          if (minNullIsValued || (!subjIsNull && !valueIsNull)) {
            // do not compare null
            aggregate.agg[i] = subjMeta.compare(subj, valueMeta, value) < 0 ? subj : value;
          } else if (valueIsNull && !subjIsNull) {
            // By default set aggregate to first not null value
            aggregate.agg[i] = subj;
          }
          break;
        case Maximum:
          if (subjMeta.compare(subj, valueMeta, value) > 0) {
            aggregate.agg[i] = subj;
          }
          break;
        case First:
          if (!subjMeta.isNull(subj) && value == null) {
            aggregate.agg[i] = subj;
          }
          break;
        case Last:
          if (!subjMeta.isNull(subj)) {
            aggregate.agg[i] = subj;
          }
          break;
        case FirstIncludingNull:
          if (aggregate.counts[i] == 0) {
            aggregate.agg[i] = subj;
            aggregate.counts[i]++;
          }
          break;
        case LastIncludingNull:
          aggregate.agg[i] = subj;
          break;
        case ConcatComma:
          if (subj != null) {
            StringBuilder sb = (StringBuilder) value;
            if (!sb.isEmpty()) {
              sb.append(", ");
            }
            sb.append(subjMeta.getString(subj));
          }
          break;
        case ConcatString:
          if (subj != null) {
            String separator = "";
            if (!Utils.isEmpty(agg.getValueField())) {
              separator = resolve(agg.getValueField());
            }
            StringBuilder sb = (StringBuilder) value;
            if (!sb.isEmpty()) {
              sb.append(separator);
            }
            sb.append(subjMeta.getString(subj));
          }
          break;
        case ConcatDistinct:
          if (subj != null) {
            SortedSet<Object> set = (SortedSet<Object>) value;
            set.add(subj);
          }
          break;
        default:
          break;
      }
    }
  }

  /**
   * Used for junits in MemoryGroupByNewAggregateTest
   *
   * @param r
   * @param aggregate
   * @throws HopException
   */
  void newAggregate(Object[] r, Aggregate aggregate) throws HopException {
    if (aggregate == null) {
      data.aggMeta = new RowMeta();
    } else {
      aggregate.counts = new long[data.subjectnrs.length];

      // Put all the counters at 0
      for (int i = 0; i < aggregate.counts.length; i++) {
        aggregate.counts[i] = 0;
      }
      aggregate.distinctObjs = null;
      aggregate.agg = new Object[data.subjectnrs.length];
      aggregate.mean = new double[data.subjectnrs.length]; // sets all doubles to 0.0
    }

    for (int i = 0; i < data.subjectnrs.length; i++) {
      IValueMeta subjMeta = data.inputRowMeta.getValueMeta(data.subjectnrs[i]);
      Object v = null;
      IValueMeta vMeta = null;
      GAggregate agg = meta.getAggregates().get(i);
      switch (agg.getType()) {
        case Median, Percentile:
          vMeta = new ValueMetaNumber(agg.getField());
          v = new ArrayList<Double>();
          break;
        case StandardDeviation:
          vMeta = new ValueMetaNumber(agg.getField());
          break;
        case CountDistinct, CountAny, CountAll:
          vMeta = new ValueMetaInteger(agg.getField());
          break;
        case Sum, Average:
          vMeta = subjMeta.isNumeric() ? subjMeta.clone() : new ValueMetaNumber();
          vMeta.setName(agg.getField());
          break;
        case First, Last, FirstIncludingNull, LastIncludingNull, Minimum, Maximum:
          vMeta = subjMeta.clone();
          vMeta.setName(agg.getField());
          v = r == null ? null : r[data.subjectnrs[i]];
          break;
        case ConcatComma, ConcatString:
          vMeta = new ValueMetaString(agg.getField());
          v = new StringBuilder();
          break;
        case ConcatDistinct:
          vMeta = new ValueMetaString(agg.getField());
          v = new TreeSet<>();
          break;
        default:
          throw new HopException("Unknown data type for aggregation : " + agg.getField());
      }

      if (agg.getType() != CountAll
          && agg.getType() != CountDistinct
          && agg.getType() != CountAny) {
        vMeta.setLength(subjMeta.getLength(), subjMeta.getPrecision());
      }
      if (aggregate == null) {
        data.aggMeta.addValueMeta(vMeta);
      } else {
        aggregate.agg[i] = v;
      }
    }
  }

  private void initGroupMeta(IRowMeta previousRowMeta) {
    data.groupMeta = new RowMeta();
    data.entryMeta = new RowMeta();

    for (int i = 0; i < data.groupnrs.length; i++) {
      IValueMeta valueMeta = previousRowMeta.getValueMeta(data.groupnrs[i]);
      data.groupMeta.addValueMeta(valueMeta);

      IValueMeta normalMeta = valueMeta.clone();
      normalMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    }
  }

  /**
   * Used for junits in MemoryGroupByAggregationNullsTest
   *
   * @param aggregate
   * @return
   * @throws HopValueException
   */
  Object[] getAggregateResult(Aggregate aggregate) throws HopValueException {
    Object[] result = new Object[data.subjectnrs.length];

    for (int i = 0; i < data.subjectnrs.length; i++) {
      GAggregate agg = meta.getAggregates().get(i);
      Object ag = aggregate.agg[i];
      switch (agg.getType()) {
        case Sum:
          break;
        case Average:
          ag =
              ValueDataUtil.divide(
                  data.aggMeta.getValueMeta(i), ag, new ValueMetaInteger("c"), aggregate.counts[i]);
          break;
        case Median, Percentile:
          double percentile = 50.0;
          if (agg.getType() == Percentile) {
            percentile = Double.parseDouble(agg.getValueField());
          }
          List<Double> valuesList = (List<Double>) aggregate.agg[i];
          double[] values = new double[valuesList.size()];
          for (int v = 0; v < values.length; v++) {
            values[v] = valuesList.get(v);
          }
          ag = new Percentile().evaluate(values, percentile);
          break;
        case CountAll, CountAny, CountDistinct:
          ag = aggregate.counts[i];
          break;
        case Minimum:
          break;
        case Maximum:
          break;
        case StandardDeviation:
          double sum = (Double) ag / aggregate.counts[i];
          ag = Math.sqrt(sum);
          break;
        case ConcatComma, ConcatString:
          ag = ((StringBuilder) ag).toString();
          break;
        case ConcatDistinct:
          IValueMeta subjMeta = data.inputRowMeta.getValueMeta(data.subjectnrs[i]);
          String separator = "";
          if (!Utils.isEmpty(agg.getValueField())) {
            separator = resolve(agg.getValueField());
          }
          StringJoiner joiner = new StringJoiner(separator);
          for (Object value : (SortedSet<Object>) ag) {
            joiner.add(subjMeta.getString(value));
          }
          ag = joiner.toString();
          break;
        default:
          break;
      }
      if (ag == null && allNullsAreZero) {
        // seems all rows for min function was nulls...
        IValueMeta vm = data.aggMeta.getValueMeta(i);
        ag = ValueDataUtil.getZeroForValueMetaType(vm);
      }
      result[i] = ag;
    }

    return result;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      data.map = new HashMap<>(5000);
      return true;
    }
    return false;
  }

  @Override
  public void dispose() {
    super.dispose();
    ((MemoryGroupByData) data).clear();
  }

  @Override
  public void batchComplete() throws HopException {
    // Empty the hash table
    //
    handleLastOfGroup();

    // Clear the complete cache...
    //
    data.map.clear();

    data.newBatch = true;
  }

  /**
   * Used for junits in MemoryGroupByAggregationNullsTest
   *
   * @param allNullsAreZero the allNullsAreZero to set
   */
  void setAllNullsAreZero(boolean allNullsAreZero) {
    this.allNullsAreZero = allNullsAreZero;
  }

  /**
   * Used for junits in MemoryGroupByAggregationNullsTest
   *
   * @param minNullIsValued the minNullIsValued to set
   */
  void setMinNullIsValued(boolean minNullIsValued) {
    this.minNullIsValued = minNullIsValued;
  }
}
