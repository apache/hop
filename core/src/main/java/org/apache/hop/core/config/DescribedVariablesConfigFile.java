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

package org.apache.hop.core.config;

import org.apache.hop.core.config.plugin.ConfigFile;

public class DescribedVariablesConfigFile extends ConfigFile implements IConfigFile {
  private String configFilename;

  public DescribedVariablesConfigFile( String configFilename ) {
    super();
    this.configFilename = configFilename;
    this.serializer = new ConfigFileSerializer();
  }

  /**
   * Gets configFilename
   *
   * @return value of configFilename
   */
  @Override public String getConfigFilename() {
    return configFilename;
  }

  /**
   * @param configFilename The configFilename to set
   */
  @Override public void setConfigFilename( String configFilename ) {
    this.configFilename = configFilename;
  }
}
