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
 *
 */

package org.apache.hop.execution.sampler.plugins.dataprof;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.ExecutionDataSamplerStoreBase;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.execution.sampler.plugins.dataprof.BasicDataProfilingDataSampler.ProfilingType;
import org.apache.hop.i18n.BaseMessages;

public class BasicDataProfilingDataSamplerStore
    extends ExecutionDataSamplerStoreBase<BasicDataProfilingDataSamplerStore>
    implements IExecutionDataSamplerStore {
  private static final Class<?> PKG = BasicDataProfilingDataSampler.class;

  protected BasicDataProfilingDataSampler dataSampler;

  /** The minimum values of fields */
  protected Map<String, Object> minValues;

  /** The minimum metadata of fields */
  protected Map<String, IValueMeta> minMeta;

  /** The maximum values of fields */
  protected Map<String, Object> maxValues;

  /** The maximum value metadata of fields */
  protected Map<String, IValueMeta> maxMeta;

  /** The counters map for null values */
  protected Map<String, Long> nullCounters;

  /** The counters map for non-null values */
  protected Map<String, Long> nonNullCounters;

  /** The minimum lengths of String fields */
  protected Map<String, Integer> minLengths;

  /** The maximum lengths of String fields */
  protected Map<String, Integer> maxLengths;

  /**
   * For each of the discovered data profiling results we can keep an original row of data in which
   * the value occurs. So for example if we find that the maximum value for field F is 'Z' we can
   * keep a number of rows in which this occurs.
   */
  protected Map<String, Map<ProfilingType, RowBuffer>> profileSamples;

  public BasicDataProfilingDataSamplerStore(
      BasicDataProfilingDataSampler dataSampler,
      ExecutionDataSamplerMeta samplerMeta,
      IRowMeta rowMeta,
      List<Object[]> rows,
      int maxRows) {
    super(samplerMeta, rowMeta, rows, maxRows);
    this.dataSampler = dataSampler;
    this.profileSamples = Collections.synchronizedMap(new HashMap<>());
    this.minValues = Collections.synchronizedMap(new HashMap<>());
    this.minMeta = Collections.synchronizedMap(new HashMap<>());
    this.maxValues = Collections.synchronizedMap(new HashMap<>());
    this.maxMeta = Collections.synchronizedMap(new HashMap<>());
    this.nullCounters = Collections.synchronizedMap(new HashMap<>());
    this.nonNullCounters = Collections.synchronizedMap(new HashMap<>());
    this.minLengths = Collections.synchronizedMap(new HashMap<>());
    this.maxLengths = Collections.synchronizedMap(new HashMap<>());
  }

  @Override
  public BasicDataProfilingDataSamplerStore getStore() {
    return this;
  }

  public BasicDataProfilingDataSamplerStore(
      BasicDataProfilingDataSampler dataSampler, ExecutionDataSamplerMeta samplerMeta) {
    this(dataSampler, samplerMeta, null, null, 0);
  }

  @Override
  public void init(IVariables variables, IRowMeta inputRowMeta, IRowMeta outputRowMeta) {
    setMaxRows(Const.toInt(variables.resolve(dataSampler.getSampleSize()), 0));
  }

  @Override
  public Map<String, RowBuffer> getSamples() {
    Map<String, RowBuffer> samples = Collections.synchronizedMap(new HashMap<>());

    String transformName = samplerMeta.getTransformName();
    String copyNr = samplerMeta.getCopyNr();

    // Profiling values
    //
    getMinValues()
        .forEach(
            (fieldName, value) ->
                samples.put(
                    createValueKey(transformName, copyNr, fieldName, ProfilingType.MinValue),
                    createRowBuffer(
                        fieldName, ProfilingType.MinValue, getMinMeta().get(fieldName), value)));

    getMaxValues()
        .forEach(
            (fieldName, value) ->
                samples.put(
                    createValueKey(transformName, copyNr, fieldName, ProfilingType.MaxValue),
                    createRowBuffer(
                        fieldName, ProfilingType.MaxValue, getMaxMeta().get(fieldName), value)));

    getMinLengths()
        .forEach(
            (fieldName, value) ->
                samples.put(
                    createValueKey(transformName, copyNr, fieldName, ProfilingType.MinLength),
                    createRowBuffer(
                        fieldName,
                        ProfilingType.MinLength,
                        new ValueMetaInteger(fieldName),
                        (long) value)));

    getMaxLengths()
        .forEach(
            (fieldName, value) ->
                samples.put(
                    createValueKey(transformName, copyNr, fieldName, ProfilingType.MaxLength),
                    createRowBuffer(
                        fieldName,
                        ProfilingType.MaxLength,
                        new ValueMetaInteger(fieldName),
                        (long) value)));
    if (dataSampler.isProfilingNrNull()) {
      getNullCounters()
          .forEach(
              (fieldName, value) ->
                  samples.put(
                      createValueKey(transformName, copyNr, fieldName, ProfilingType.NrNulls),
                      createRowBuffer(
                          fieldName,
                          ProfilingType.NrNulls,
                          new ValueMetaInteger(fieldName),
                          value)));
    }
    if (dataSampler.isProfilingNrNonNull()) {
      getNonNullCounters()
          .forEach(
              (fieldName, value) ->
                  samples.put(
                      createValueKey(transformName, copyNr, fieldName, ProfilingType.NrNonNulls),
                      createRowBuffer(
                          fieldName,
                          ProfilingType.NrNonNulls,
                          new ValueMetaInteger(fieldName),
                          value)));
    }

    // Wrap up the sample rows we have
    //
    for (String fieldName : getProfileSamples().keySet()) {
      Map<ProfilingType, RowBuffer> dataMap = getProfileSamples().get(fieldName);
      for (ProfilingType profilingType : ProfilingType.values()) {
        RowBuffer rowBuffer = dataMap.get(profilingType);
        if (rowBuffer != null && !rowBuffer.isEmpty()) {
          String samplesKey = createSamplesKey(transformName, copyNr, fieldName, profilingType);
          samples.put(samplesKey, rowBuffer);
        }
      }
    }

    return samples;
  }

  @Override
  public Map<String, ExecutionDataSetMeta> getSamplesMetadata() {
    Map<String, ExecutionDataSetMeta> map = Collections.synchronizedMap(new HashMap<>());

    String transformName = samplerMeta.getTransformName();
    String copyNr = samplerMeta.getCopyNr();

    // Profiling values
    //
    getMinValues()
        .forEach(
            (fieldName, value) ->
                map.put(
                    createValueKey(transformName, copyNr, fieldName, ProfilingType.MinValue),
                    createValueMeta(fieldName, ProfilingType.MinValue)));

    getMaxValues()
        .forEach(
            (fieldName, value) ->
                map.put(
                    createValueKey(transformName, copyNr, fieldName, ProfilingType.MaxValue),
                    createValueMeta(fieldName, ProfilingType.MaxValue)));

    getMinLengths()
        .forEach(
            (fieldName, value) ->
                map.put(
                    createValueKey(transformName, copyNr, fieldName, ProfilingType.MinLength),
                    createValueMeta(fieldName, ProfilingType.MinLength)));
    getMaxLengths()
        .forEach(
            (fieldName, value) ->
                map.put(
                    createValueKey(transformName, copyNr, fieldName, ProfilingType.MaxLength),
                    createValueMeta(fieldName, ProfilingType.MaxLength)));
    if (dataSampler.isProfilingNrNull()) {
      getNullCounters()
          .forEach(
              (fieldName, value) ->
                  map.put(
                      createValueKey(transformName, copyNr, fieldName, ProfilingType.NrNulls),
                      createValueMeta(fieldName, ProfilingType.NrNulls)));
    }
    if (dataSampler.isProfilingNrNonNull()) {
      getNonNullCounters()
          .forEach(
              (fieldName, value) ->
                  map.put(
                      createValueKey(transformName, copyNr, fieldName, ProfilingType.NrNonNulls),
                      createValueMeta(fieldName, ProfilingType.NrNonNulls)));
    }

    // Sample rows
    //
    for (String fieldName : getProfileSamples().keySet()) {
      Map<ProfilingType, RowBuffer> dataMap = getProfileSamples().get(fieldName);
      synchronized (map) {
        synchronized (dataMap) {
          for (ProfilingType profilingType : ProfilingType.values()) {
            String samplesKey =
                createSamplesKey(
                    samplerMeta.getTransformName(),
                    samplerMeta.getCopyNr(),
                    fieldName,
                    profilingType);
            String samplesDescription =
                createSamplesDescription(
                    samplerMeta.getTransformName(),
                    samplerMeta.getCopyNr(),
                    fieldName,
                    profilingType);
            ExecutionDataSetMeta setMeta =
                new ExecutionDataSetMeta(
                    samplesKey,
                    samplerMeta.getLogChannelId(),
                    samplerMeta.getTransformName(),
                    samplerMeta.getCopyNr(),
                    fieldName,
                    profilingType.getDescription(),
                    samplesDescription);
            map.put(samplesKey, setMeta);
          }
        }
      }
    }

    return map;
  }

  /**
   * Create a standard row buffer with a single value and a single row in it.
   *
   * @param fieldName The field name to use
   * @param profilingType The type of profiling used
   * @param valueMeta The value metadata of the profiling result
   * @param valueData The profiling result for the column
   * @return The row buffer
   */
  private RowBuffer createRowBuffer(
      String fieldName, ProfilingType profilingType, IValueMeta valueMeta, Object valueData) {
    IRowMeta bufferRowMeta = new RowMeta();
    bufferRowMeta.addValueMeta(valueMeta);
    Object[] bufferRow = RowDataUtil.allocateRowData(1);
    bufferRow[0] = valueData;
    return new RowBuffer(bufferRowMeta, List.<Object[]>of(bufferRow));
  }

  private String createValueKey(
      String transformName, String copyNr, String fieldName, ProfilingType profilingType) {
    return transformName + "." + copyNr + ": " + profilingType.name() + "-value-" + fieldName;
  }

  private ExecutionDataSetMeta createValueMeta(String fieldName, ProfilingType profilingType) {
    String setKey =
        createValueKey(
            samplerMeta.getTransformName(), samplerMeta.getCopyNr(), fieldName, profilingType);
    String setDescription = profilingType.getDescription() + " : " + fieldName;
    return new ExecutionDataSetMeta(
        setKey,
        samplerMeta.getLogChannelId(),
        samplerMeta.getTransformName(),
        samplerMeta.getCopyNr(),
        fieldName,
        profilingType.getDescription(),
        setDescription);
  }

  private String createSamplesKey(
      String transformName, String copyNr, String fieldName, ProfilingType profilingType) {
    return transformName + "." + copyNr + ": " + profilingType.name() + "-samples-" + fieldName;
  }

  private String createSamplesDescription(
      String transformName, String copyNr, String fieldName, ProfilingType profilingType) {
    return BaseMessages.getString(
        PKG,
        "BasicDataProfilingRowsExecutionDataSample.SamplesDescription",
        transformName,
        copyNr,
        profilingType.getDescription(),
        fieldName);
  }

  /**
   * Gets minValues
   *
   * @return value of minValues
   */
  public Map<String, Object> getMinValues() {
    return minValues;
  }

  /**
   * Sets minValues
   *
   * @param minValues value of minValues
   */
  public void setMinValues(Map<String, Object> minValues) {
    this.minValues = minValues;
  }

  /**
   * Gets minMeta
   *
   * @return value of minMeta
   */
  public Map<String, IValueMeta> getMinMeta() {
    return minMeta;
  }

  /**
   * Sets minMeta
   *
   * @param minMeta value of minMeta
   */
  public void setMinMeta(Map<String, IValueMeta> minMeta) {
    this.minMeta = minMeta;
  }

  /**
   * Gets maxValues
   *
   * @return value of maxValues
   */
  public Map<String, Object> getMaxValues() {
    return maxValues;
  }

  /**
   * Sets maxValues
   *
   * @param maxValues value of maxValues
   */
  public void setMaxValues(Map<String, Object> maxValues) {
    this.maxValues = maxValues;
  }

  /**
   * Gets maxMeta
   *
   * @return value of maxMeta
   */
  public Map<String, IValueMeta> getMaxMeta() {
    return maxMeta;
  }

  /**
   * Sets maxMeta
   *
   * @param maxMeta value of maxMeta
   */
  public void setMaxMeta(Map<String, IValueMeta> maxMeta) {
    this.maxMeta = maxMeta;
  }

  /**
   * Gets nullCounters
   *
   * @return value of nullCounters
   */
  public Map<String, Long> getNullCounters() {
    return nullCounters;
  }

  /**
   * Sets nullCounters
   *
   * @param nullCounters value of nullCounters
   */
  public void setNullCounters(Map<String, Long> nullCounters) {
    this.nullCounters = nullCounters;
  }

  /**
   * Gets nonNullCounters
   *
   * @return value of nonNullCounters
   */
  public Map<String, Long> getNonNullCounters() {
    return nonNullCounters;
  }

  /**
   * Sets nonNullCounters
   *
   * @param nonNullCounters value of nonNullCounters
   */
  public void setNonNullCounters(Map<String, Long> nonNullCounters) {
    this.nonNullCounters = nonNullCounters;
  }

  /**
   * Gets minLengths
   *
   * @return value of minLengths
   */
  public Map<String, Integer> getMinLengths() {
    return minLengths;
  }

  /**
   * Sets minLengths
   *
   * @param minLengths value of minLengths
   */
  public void setMinLengths(Map<String, Integer> minLengths) {
    this.minLengths = minLengths;
  }

  /**
   * Gets maxLengths
   *
   * @return value of maxLengths
   */
  public Map<String, Integer> getMaxLengths() {
    return maxLengths;
  }

  /**
   * Sets maxLengths
   *
   * @param maxLengths value of maxLengths
   */
  public void setMaxLengths(Map<String, Integer> maxLengths) {
    this.maxLengths = maxLengths;
  }

  /**
   * Gets profileSamples
   *
   * @return value of profileSamples
   */
  public Map<String, Map<ProfilingType, RowBuffer>> getProfileSamples() {
    return profileSamples;
  }

  /**
   * Sets profileSamples
   *
   * @param profileSamples value of profileSamples
   */
  public void setProfileSamples(Map<String, Map<ProfilingType, RowBuffer>> profileSamples) {
    this.profileSamples = profileSamples;
  }
}
