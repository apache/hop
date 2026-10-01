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

package org.apache.hop.setup.persist;

import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.setup.HopEnvironmentApplyResult;
import org.apache.hop.setup.HopEnvironmentSpec;
import org.apache.hop.setup.HopSetupException;
import org.apache.hop.setup.HopSetupVariables;

/** Creates config/audit folders and optionally copies {@code <install>/config}. */
public class ConfigFolderSeeder {

  public void seed(HopEnvironmentSpec spec, HopEnvironmentApplyResult result)
      throws HopSetupException {
    seed(spec, result, HopInstallHome.resolveOrNull());
  }

  public void seed(HopEnvironmentSpec spec, HopEnvironmentApplyResult result, Path hopHome)
      throws HopSetupException {
    if (spec.isCreateFolders()) {
      createIfSet(spec.getConfigFolder(), result);
      createIfSet(spec.getAuditFolder(), result);
    }
    if (!spec.isCopyExisting()) {
      return;
    }
    if (StringUtils.isBlank(spec.getConfigFolder())) {
      return;
    }
    String targetConfig = spec.getConfigFolder() + "/" + HopSetupVariables.HOP_CONFIG_JSON;
    if (HopVfsFiles.exists(targetConfig)) {
      result.addMessage(
          "Skipped copy of install config: "
              + HopSetupVariables.HOP_CONFIG_JSON
              + " already exists in the target folder");
      return;
    }
    if (hopHome == null) {
      result.addMessage("Skipped copy of install config: Hop install directory not found");
      return;
    }
    String source = hopHome.resolve("config").toString();
    String sourceConfig =
        hopHome.resolve("config").resolve(HopSetupVariables.HOP_CONFIG_JSON).toString();
    if (!HopVfsFiles.exists(sourceConfig)) {
      result.addMessage("Skipped copy of install config: " + sourceConfig + " not found");
      return;
    }
    if (spec.isDryRun()) {
      result.addMessage("Would copy " + source + " to " + spec.getConfigFolder());
      return;
    }
    HopVfsFiles.copyTree(source, spec.getConfigFolder());
    result.addMessage("Copied install config from " + source + " to " + spec.getConfigFolder());
  }

  private void createIfSet(String folder, HopEnvironmentApplyResult result)
      throws HopSetupException {
    if (StringUtils.isBlank(folder)) {
      return;
    }
    if (HopVfsFiles.exists(folder)) {
      return;
    }
    if (result.isDryRun()) {
      result.addMessage("Would create folder " + folder);
      return;
    }
    HopVfsFiles.createFolder(folder);
    result.addMessage("Created folder " + folder);
  }
}
