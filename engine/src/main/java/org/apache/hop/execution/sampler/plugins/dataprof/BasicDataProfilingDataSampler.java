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

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.ExecutionDataSamplerPlugin;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.stream.IStream;

@GuiPlugin
@ExecutionDataSamplerPlugin(
    id = "BasicDataProfilingRowsExecutionDataSampler",
    name = "Data profile output rows",
    description = "Allow for some basic data profiling to be performed on transform output rows")
public class BasicDataProfilingDataSampler
    implements IExecutionDataSampler<BasicDataProfilingDataSamplerStore> {
  private static final Class<?> PKG = BasicDataProfilingDataSampler.class;

  @SuppressWarnings("java:S115")
  public enum ProfilingType {
    MinValue(BaseMessages.getString(PKG, "BasicDataProfilingDataSampler.Label.MinValue")),
    MaxValue(BaseMessages.getString(PKG, "BasicDataProfilingDataSampler.Label.MaxValue")),
    NrNulls(BaseMessages.getString(PKG, "BasicDataProfilingDataSampler.Label.NrNulls")),
    NrNonNulls(BaseMessages.getString(PKG, "BasicDataProfilingDataSampler.Label.NrNonNulls")),
    MinLength(BaseMessages.getString(PKG, "BasicDataProfilingDataSampler.Label.MinLength")),
    MaxLength(BaseMessages.getString(PKG, "BasicDataProfilingDataSampler.Label.MaxLength"));

    private final String description;

    ProfilingType(String description) {
      this.description = description;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }
  }

  private String pluginId;
  private String pluginName;

  // The metadata fields
  //

  /** The number of rows to sample for each field and each profiling result. */
  @GuiWidgetElement(
      order = "100",
      type = GuiElementType.TEXT,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BasicDataProfilingDataSampler.Label.SampleSize",
      toolTip = "i18n::BasicDataProfilingDataSampler.Tooltip.SampleSize")
  @HopMetadataProperty
  protected String sampleSize;

  @GuiWidgetElement(
      order = "105",
      type = GuiElementType.CHECKBOX,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BasicDataProfilingDataSampler.Label.LastTransforms")
  @HopMetadataProperty
  protected boolean onlyProfilingLastTransforms;

  @GuiWidgetElement(
      order = "110",
      type = GuiElementType.CHECKBOX,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BasicDataProfilingDataSampler.Label.ProfilingMinimum")
  @HopMetadataProperty
  protected boolean profilingMinimum;

  @GuiWidgetElement(
      order = "120",
      type = GuiElementType.CHECKBOX,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BasicDataProfilingDataSampler.Label.ProfilingMaximum")
  @HopMetadataProperty
  protected boolean profilingMaximum;

  @GuiWidgetElement(
      order = "140",
      type = GuiElementType.CHECKBOX,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BasicDataProfilingDataSampler.Label.ProfilingNrNull")
  @HopMetadataProperty
  protected boolean profilingNrNull;

  @GuiWidgetElement(
      order = "150",
      type = GuiElementType.CHECKBOX,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BasicDataProfilingDataSampler.Label.ProfilingNrNonNull")
  @HopMetadataProperty
  protected boolean profilingNrNonNull;

  @GuiWidgetElement(
      order = "160",
      type = GuiElementType.CHECKBOX,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BasicDataProfilingDataSampler.Label.ProfilingMinimumLength")
  @HopMetadataProperty
  protected boolean profilingMinLength;

  @GuiWidgetElement(
      order = "170",
      type = GuiElementType.CHECKBOX,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BasicDataProfilingDataSampler.Label.ProfilingMaximumLength")
  @HopMetadataProperty
  protected boolean profilingMaxLength;

  public BasicDataProfilingDataSampler() {
    this.sampleSize = "25";
    this.onlyProfilingLastTransforms = true;
    this.profilingMinimum = true;
    this.profilingMaximum = true;
    this.profilingNrNull = true;
    this.profilingNrNonNull = true;
    this.profilingMinLength = true;
    this.profilingMaxLength = true;
  }

  public BasicDataProfilingDataSampler(BasicDataProfilingDataSampler sampler) {
    this.pluginId = sampler.pluginId;
    this.pluginName = sampler.pluginName;
    this.sampleSize = sampler.sampleSize;
    this.onlyProfilingLastTransforms = sampler.onlyProfilingLastTransforms;
    this.profilingMinimum = sampler.profilingMinimum;
    this.profilingMaximum = sampler.profilingMaximum;
    this.profilingNrNull = sampler.profilingNrNull;
    this.profilingNrNonNull = sampler.profilingNrNonNull;
    this.profilingMinLength = sampler.profilingMinLength;
    this.profilingMaxLength = sampler.profilingMaxLength;
  }

  public BasicDataProfilingDataSampler clone() {
    return new BasicDataProfilingDataSampler(this);
  }

  @Override
  public BasicDataProfilingDataSamplerStore createSamplerStore(
      ExecutionDataSamplerMeta samplerMeta) {
    return new BasicDataProfilingDataSamplerStore(this, samplerMeta);
  }

  @Override
  public void sampleRow(
      BasicDataProfilingDataSamplerStore store,
      IStream.StreamType streamType,
      IRowMeta rowMeta,
      Object[] row)
      throws HopException {

    if (streamType != IStream.StreamType.OUTPUT) {
      return;
    }

    // By default, we only do data profiling on the end-points of a pipeline, the last transforms.
    //
    if (onlyProfilingLastTransforms && !store.getSamplerMeta().isLastTransform()) {
      return;
    }

    try {

      // Profile all columns
      //
      for (int i = 0; i < rowMeta.size(); i++) {
        IValueMeta valueMeta = rowMeta.getValueMeta(i);
        Object valueData = row[i];
        String name = valueMeta.getName();

        if (valueMeta.isNull(valueData)) {
          if (profilingNrNull) {
            long counter = store.getNullCounters().getOrDefault(name, 0L);
            store.getNullCounters().put(name, ++counter);
            addSampleRow(store, name, ProfilingType.NrNulls, rowMeta, row);
          }
        } else {
          if (profilingNrNonNull) {
            long counter = store.getNonNullCounters().getOrDefault(name, 0L);
            store.getNonNullCounters().put(name, ++counter);
            addSampleRow(store, name, ProfilingType.NrNonNulls, rowMeta, row);
          }
        }

        // Minimum
        //
        if (profilingMinimum) {
          Object oldMin = store.getMinValues().get(name);
          if (oldMin == null) {
            store.getMinValues().put(name, valueData);
            store.getMinMeta().put(name, valueMeta);
          } else {
            int compare = valueMeta.compare(valueData, oldMin);
            if (compare < 0) {
              // We have a new minimum
              //
              store.getMinValues().put(name, valueData);
              store.getMinMeta().put(name, valueMeta);

              clearSampleRows(store, name, ProfilingType.MinValue);

              // Also save the row of data as a sample
              //
              addSampleRow(store, name, ProfilingType.MinValue, rowMeta, row);
            } else if (compare == 0) {
              // We found another value at the current minimum
              addSampleRow(store, name, ProfilingType.MinValue, rowMeta, row);
            }
          }
        }

        // Maximum
        //
        if (profilingMaximum) {
          Object oldMax = store.getMaxValues().get(name);
          if (oldMax == null) {
            store.getMaxValues().put(name, valueData);
            store.getMaxMeta().put(name, valueMeta);
          } else {
            int compare = valueMeta.compare(valueData, oldMax);
            if (compare > 0) {
              // We have a new maximum
              //
              store.getMaxValues().put(name, valueData);
              store.getMaxMeta().put(name, valueMeta);

              clearSampleRows(store, name, ProfilingType.MaxValue);

              // Also save the row of data as a sample
              //
              addSampleRow(store, name, ProfilingType.MaxValue, rowMeta, row);
            } else if (compare == 0) {
              // We found another value at the current maximum
              addSampleRow(store, name, ProfilingType.MaxValue, rowMeta, row);
            }
          }
        }

        // Strings only
        //
        if (valueMeta.isString() && !valueMeta.isNull(valueData)) {
          // Minimum length
          //
          if (profilingMinLength) {
            String string = valueMeta.getString(valueData);
            int length = string.length();

            Integer oldMin = store.getMinLengths().get(name);
            if (oldMin == null) {
              store.getMinLengths().put(name, length);
            } else {
              if (length < oldMin) {
                // We have a new minimum length
                //
                store.getMinLengths().put(name, length);

                clearSampleRows(store, name, ProfilingType.MinLength);

                // Also save the row of data as a sample
                //
                addSampleRow(store, name, ProfilingType.MinLength, rowMeta, row);
              } else if (length == oldMin) {
                // We found another value at the current minimum length
                addSampleRow(store, name, ProfilingType.MinLength, rowMeta, row);
              }
            }
          }

          // Maximum length
          //
          if (profilingMaxLength) {
            String string = valueMeta.getString(valueData);
            int length = string.length();

            Integer oldMax = store.getMaxLengths().get(name);
            if (oldMax == null) {
              store.getMaxLengths().put(name, length);
            } else {
              if (length > oldMax) {
                // We have a new maximum length
                //
                store.getMaxLengths().put(name, length);

                clearSampleRows(store, name, ProfilingType.MaxLength);

                // Also save the row of data as a sample
                //
                addSampleRow(store, name, ProfilingType.MaxLength, rowMeta, row);
              } else if (length == oldMax) {
                // We found another value at the current maximum length
                addSampleRow(store, name, ProfilingType.MaxLength, rowMeta, row);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error data profiling row " + rowMeta.toStringMeta(), e);
    }
  }

  private void clearSampleRows(
      BasicDataProfilingDataSamplerStore store, String name, ProfilingType profilingType) {
    Map<ProfilingType, RowBuffer> typeBufferMap = store.getProfileSamples().get(name);
    if (typeBufferMap != null) {
      typeBufferMap.remove(profilingType);
    }
  }

  private void addSampleRow(
      BasicDataProfilingDataSamplerStore store,
      String name,
      ProfilingType profilingType,
      IRowMeta rowMeta,
      Object[] row) {
    synchronized (store.getProfileSamples()) {
      Map<ProfilingType, RowBuffer> typeBufferMap =
          store.getProfileSamples().computeIfAbsent(name, k -> new HashMap<>());
      RowBuffer rowBuffer =
          typeBufferMap.computeIfAbsent(profilingType, k -> new RowBuffer(rowMeta));

      // Keep the memory consumption sane
      //
      if (rowBuffer.size() < store.getMaxRows()) {
        rowBuffer.addRow(row);
      }
    }
  }

  /**
   * Gets pluginId
   *
   * @return value of pluginId
   */
  @Override
  public String getPluginId() {
    return pluginId;
  }

  /**
   * Sets pluginId
   *
   * @param pluginId value of pluginId
   */
  @Override
  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  /**
   * Gets pluginName
   *
   * @return value of pluginName
   */
  @Override
  public String getPluginName() {
    return pluginName;
  }

  /**
   * Sets pluginName
   *
   * @param pluginName value of pluginName
   */
  @Override
  public void setPluginName(String pluginName) {
    this.pluginName = pluginName;
  }

  /**
   * Gets sampleSize
   *
   * @return value of sampleSize
   */
  public String getSampleSize() {
    return sampleSize;
  }

  /**
   * Sets sampleSize
   *
   * @param sampleSize value of sampleSize
   */
  public void setSampleSize(String sampleSize) {
    this.sampleSize = sampleSize;
  }

  /**
   * Gets onlyProfilingLastTransforms
   *
   * @return value of onlyProfilingLastTransforms
   */
  public boolean isOnlyProfilingLastTransforms() {
    return onlyProfilingLastTransforms;
  }

  /**
   * Sets onlyProfilingLastTransforms
   *
   * @param onlyProfilingLastTransforms value of onlyProfilingLastTransforms
   */
  public void setOnlyProfilingLastTransforms(boolean onlyProfilingLastTransforms) {
    this.onlyProfilingLastTransforms = onlyProfilingLastTransforms;
  }

  /**
   * Gets profilingMinimum
   *
   * @return value of profilingMinimum
   */
  public boolean isProfilingMinimum() {
    return profilingMinimum;
  }

  /**
   * Sets profilingMinimum
   *
   * @param profilingMinimum value of profilingMinimum
   */
  public void setProfilingMinimum(boolean profilingMinimum) {
    this.profilingMinimum = profilingMinimum;
  }

  /**
   * Gets profilingMaximum
   *
   * @return value of profilingMaximum
   */
  public boolean isProfilingMaximum() {
    return profilingMaximum;
  }

  /**
   * Sets profilingMaximum
   *
   * @param profilingMaximum value of profilingMaximum
   */
  public void setProfilingMaximum(boolean profilingMaximum) {
    this.profilingMaximum = profilingMaximum;
  }

  /**
   * Gets profilingNrNull
   *
   * @return value of profilingNrNull
   */
  public boolean isProfilingNrNull() {
    return profilingNrNull;
  }

  /**
   * Sets profilingNrNull
   *
   * @param profilingNrNull value of profilingNrNull
   */
  public void setProfilingNrNull(boolean profilingNrNull) {
    this.profilingNrNull = profilingNrNull;
  }

  /**
   * Gets profilingNrNonNull
   *
   * @return value of profilingNrNonNull
   */
  public boolean isProfilingNrNonNull() {
    return profilingNrNonNull;
  }

  /**
   * Sets profilingNrNonNull
   *
   * @param profilingNrNonNull value of profilingNrNonNull
   */
  public void setProfilingNrNonNull(boolean profilingNrNonNull) {
    this.profilingNrNonNull = profilingNrNonNull;
  }

  /**
   * Gets profilingMinLength
   *
   * @return value of profilingMinLength
   */
  public boolean isProfilingMinLength() {
    return profilingMinLength;
  }

  /**
   * Sets profilingMinLength
   *
   * @param profilingMinLength value of profilingMinLength
   */
  public void setProfilingMinLength(boolean profilingMinLength) {
    this.profilingMinLength = profilingMinLength;
  }

  /**
   * Gets profilingMaxLength
   *
   * @return value of profilingMaxLength
   */
  public boolean isProfilingMaxLength() {
    return profilingMaxLength;
  }

  /**
   * Sets profilingMaxLength
   *
   * @param profilingMaxLength value of profilingMaxLength
   */
  public void setProfilingMaxLength(boolean profilingMaxLength) {
    this.profilingMaxLength = profilingMaxLength;
  }
}
