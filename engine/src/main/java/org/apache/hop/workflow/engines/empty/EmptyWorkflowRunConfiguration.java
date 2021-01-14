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

package org.apache.hop.workflow.engines.empty;

import org.apache.commons.validator.Var;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;

import java.util.Objects;

public class EmptyWorkflowRunConfiguration extends Variables implements IWorkflowEngineRunConfiguration, Cloneable {

  private String pluginId;
  private String pluginName;

  public EmptyWorkflowRunConfiguration() {
  }

  public EmptyWorkflowRunConfiguration( String pluginId, String pluginName ) {
    this.pluginId = pluginId;
    this.pluginName = pluginName;
  }

  public EmptyWorkflowRunConfiguration( EmptyWorkflowRunConfiguration config ) {
    this.pluginId = config.pluginId;
    this.pluginName = config.pluginName;
  }

  public EmptyWorkflowRunConfiguration clone() {
    return new EmptyWorkflowRunConfiguration( this );
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    EmptyWorkflowRunConfiguration that = (EmptyWorkflowRunConfiguration) o;
    return pluginId.equals( that.pluginId );
  }

  @Override public int hashCode() {
    return Objects.hash( pluginId );
  }

  /**
   * Gets pluginId
   *
   * @return value of pluginId
   */
  public String getEnginePluginId() {
    return pluginId;
  }

  /**
   * @param pluginId The pluginId to set
   */
  @Override public void setEnginePluginId( String pluginId ) {
    this.pluginId = pluginId;
  }

  /**
   * Gets pluginName
   *
   * @return value of pluginName
   */
  public String getEnginePluginName() {
    return pluginName;
  }

  /**
   * @param pluginName The pluginName to set
   */
  @Override public void setEnginePluginName( String pluginName ) {
    this.pluginName = pluginName;
  }
}
