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
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;

/**
 * We need out mapping to be parameterized.<br>
 * This we do with the use of environment variables.<br>
 * That way we can set one variable to another, etc.<br>
 */
public class MappingParameters implements Cloneable {

  @HopMetadataProperty(key = "variablemapping")
  private List<MappingVariableMapping> variableMappings;

  /** This flag causes the sub-transformation to inherit all variables from the parent */
  @HopMetadataProperty(key = "inherit_all_vars")
  private boolean inheritingAllVariables;

  public MappingParameters() {
    super();
    variableMappings = new ArrayList<>();
    inheritingAllVariables = true;
  }

  public MappingParameters(MappingParameters p) {
    this();
    this.inheritingAllVariables = p.inheritingAllVariables;
    for (MappingVariableMapping mapping : p.variableMappings) {
      variableMappings.add(new MappingVariableMapping(mapping));
    }
  }

  public String[] getVariables() {
    String[] vars = new String[variableMappings.size()];
    for (int i = 0; i < vars.length; i++) {
      vars[i] = variableMappings.get(i).getName();
    }
    return vars;
  }

  public String[] getInputs() {
    String[] input = new String[variableMappings.size()];
    for (int i = 0; i < input.length; i++) {
      input[i] = variableMappings.get(i).getValue();
    }
    return input;
  }

  @Override
  public MappingParameters clone() {
    return new MappingParameters(this);
  }

  public String getXml() throws HopException {
    return XmlMetadataUtil.serializeObjectToXml(this);
  }

  /**
   * Gets mappings
   *
   * @return value of mappings
   */
  public List<MappingVariableMapping> getVariableMappings() {
    return variableMappings;
  }

  /**
   * Sets mappings
   *
   * @param variableMappings value of mappings
   */
  public void setVariableMappings(List<MappingVariableMapping> variableMappings) {
    this.variableMappings = variableMappings;
  }

  /**
   * @return the inheritingAllVariables
   */
  public boolean isInheritingAllVariables() {
    return inheritingAllVariables;
  }

  /**
   * @param inheritingAllVariables the inheritingAllVariables to set
   */
  public void setInheritingAllVariables(boolean inheritingAllVariables) {
    this.inheritingAllVariables = inheritingAllVariables;
  }
}
