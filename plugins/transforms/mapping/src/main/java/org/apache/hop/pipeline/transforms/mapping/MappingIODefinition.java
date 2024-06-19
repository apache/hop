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

package org.apache.hop.pipeline.transforms.mapping;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

/** Helps to define the input or output specifications for the Mapping transform. */
@HopMetadataWrapper(tag = "mapping")
public class MappingIODefinition implements Cloneable {

  public static final String XML_TAG = "mapping";

  @SuppressWarnings("java:S2065")
  private transient TransformMeta inputTransform;

  @HopMetadataProperty(key = "input_transform")
  private String inputTransformName;

  @HopMetadataProperty(key = "output_transform")
  private String outputTransformName;

  @HopMetadataProperty(key = "description")
  private String description;

  @HopMetadataProperty(key = "connector")
  private List<MappingValueRename> valueRenames;

  @HopMetadataProperty(key = "main_path")
  private boolean mainDataPath;

  @HopMetadataProperty(key = "rename_on_output")
  private boolean renamingOnOutput;

  /**
   * No input or output transform is defined:<br>
   * - detect the source transform automatically: use all input transforms for this mapping
   * transform.<br>
   * - detect the output transform automatically: there can only be one MappingInput transform in
   * the mapping in this specific case.
   */
  public MappingIODefinition() {
    super();
    this.inputTransformName = null;
    this.outputTransformName = null;
    this.valueRenames = new ArrayList<>();
    this.mainDataPath = false;
    this.renamingOnOutput = false;
  }

  /**
   * @param inputTransformName the name of the transform to "connect" to. If no name is given,
   *     detect the source transform automatically: use all input transforms for this mapping
   *     transform.
   * @param outputTransformName the name of the transform in the mapping to accept the data from the
   *     input transform. If no name is given, detect the output transform automatically: there can
   *     only be one MappingInput transform in the mapping in this specific case.
   */
  public MappingIODefinition(String inputTransformName, String outputTransformName) {
    this();
    this.inputTransformName = inputTransformName;
    this.outputTransformName = outputTransformName;
  }

  public MappingIODefinition(MappingIODefinition d) {
    this();
    this.inputTransformName = d.inputTransformName;
    this.outputTransformName = d.outputTransformName;
    this.description = d.description;
    this.mainDataPath = d.mainDataPath;
    this.renamingOnOutput = d.renamingOnOutput;
    for (MappingValueRename rename : d.valueRenames) {
      this.valueRenames.add(new MappingValueRename(rename));
    }
  }

  @Override
  public MappingIODefinition clone() {
    return new MappingIODefinition(this);
  }

  public MappingIODefinition(Node mappingNode) throws HopXmlException {
    this();

    XmlMetadataUtil.deSerializeFromXml(
        this, mappingNode, MappingIODefinition.class, new MemoryMetadataProvider());
  }

  public String getXml() throws HopException {

    return XmlMetadataUtil.serializeObjectToXml(this);
  }

  /**
   * @return the TransformName, the name of the transform to "connect" to. If no transform name is
   *     given, detect the Mapping Input/Output transform automatically.
   */
  public String getInputTransformName() {
    return inputTransformName;
  }

  /**
   * @param inputTransformName the TransformName to set
   */
  public void setInputTransformName(String inputTransformName) {
    this.inputTransformName = inputTransformName;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description the description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * @return the outputTransformName
   */
  public String getOutputTransformName() {
    return outputTransformName;
  }

  /**
   * @param outputTransformName the outputTransformName to set
   */
  public void setOutputTransformName(String outputTransformName) {
    this.outputTransformName = outputTransformName;
  }

  /**
   * @return true if this is the main data path for the mapping transform.
   */
  public boolean isMainDataPath() {
    return mainDataPath;
  }

  /**
   * @param mainDataPath true if this is the main data path for the mapping transform.
   */
  public void setMainDataPath(boolean mainDataPath) {
    this.mainDataPath = mainDataPath;
  }

  /**
   * @return the renamingOnOutput
   */
  public boolean isRenamingOnOutput() {
    return renamingOnOutput;
  }

  /**
   * @param renamingOnOutput the renamingOnOutput to set
   */
  public void setRenamingOnOutput(boolean renamingOnOutput) {
    this.renamingOnOutput = renamingOnOutput;
  }

  /**
   * @return the valueRenames
   */
  public List<MappingValueRename> getValueRenames() {
    return valueRenames;
  }

  /**
   * @param valueRenames the valueRenames to set
   */
  public void setValueRenames(List<MappingValueRename> valueRenames) {
    this.valueRenames = valueRenames;
  }

  /**
   * Gets inputTransform
   *
   * @return value of inputTransform
   */
  public TransformMeta getInputTransform() {
    return inputTransform;
  }

  /**
   * @param inputTransform The inputTransform to set
   */
  public void setInputTransform(TransformMeta inputTransform) {
    this.inputTransform = inputTransform;
  }
}
