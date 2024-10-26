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

package org.apache.hop.www.async;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "async-web-service",
    name = "i18n::AsynWebService.name",
    description = "i18n::AsynWebService.description",
    image = "ui/images/server.svg",
    documentationUrl = "metadata-types/async-web-service.html",
    hopMetadataPropertyType = HopMetadataPropertyType.SERVER_WEB_SERVICE_ASYNC)
public class AsyncWebService extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty private boolean enabled;
  @HopMetadataProperty private String filename;
  @HopMetadataProperty private String statusVariables;
  @HopMetadataProperty private String bodyContentVariable;
  @HopMetadataProperty private String runConfigurationName;

  public AsyncWebService() {
    this.enabled = true;
    this.bodyContentVariable = "ASYNC_CONTENT";
  }

  public AsyncWebService(
      String name,
      boolean enabled,
      String filename,
      String statusVariables,
      String bodyContentVariable,
      String runConfigurationName) {
    super(name);
    this.enabled = enabled;
    this.filename = filename;
    this.statusVariables = statusVariables;
    this.bodyContentVariable = bodyContentVariable;
    this.runConfigurationName = runConfigurationName;
  }

  /**
   * Split the status variables parameter using a comma (,) separator
   *
   * @param variables To resolve any variables used
   * @return The list of status variables
   */
  public List<String> getStatusVariablesList(IVariables variables) {
    List<String> list = new ArrayList<>();
    String realVars = variables.resolve(statusVariables);
    if (StringUtils.isNotEmpty(realVars)) {
      String[] vars = realVars.split(",");
      for (String var : vars) {
        list.add(Const.trim(var));
      }
    }
    return list;
  }

  /**
   * Gets enabled
   *
   * @return value of enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @param enabled The enabled to set
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * Gets statusVariables
   *
   * @return value of statusVariables
   */
  public String getStatusVariables() {
    return statusVariables;
  }

  /**
   * @param statusVariables The statusVariables to set
   */
  public void setStatusVariables(String statusVariables) {
    this.statusVariables = statusVariables;
  }

  /**
   * Gets bodyContentVariable
   *
   * @return value of bodyContentVariable
   */
  public String getBodyContentVariable() {
    return bodyContentVariable;
  }

  /**
   * @param bodyContentVariable The bodyContentVariable to set
   */
  public void setBodyContentVariable(String bodyContentVariable) {
    this.bodyContentVariable = bodyContentVariable;
  }

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /**
   * Sets runConfigurationName
   *
   * @param runConfigurationName value of runConfigurationName
   */
  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }
}
