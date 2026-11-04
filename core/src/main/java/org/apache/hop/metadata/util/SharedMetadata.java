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

package org.apache.hop.metadata.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.SharedMetadataProvider;

/** Loads the shared metadata folder from {@code hop-config.json} into one memory provider. */
public final class SharedMetadata {

  public static final String CONFIG_KEY = "sharedMetadataFolder";

  /** Used when an import has to choose a folder and none is configured yet. */
  public static final String DEFAULT_FOLDER = "${HOP_CONFIG_FOLDER}/metadata";

  private static final String VARIABLE = "HOP_CONFIG_FOLDER";

  private SharedMetadata() {}

  /**
   * Read {@link #CONFIG_KEY}. A blank value clears the shared provider. A folder whose scheme is a
   * named VFS connection is rejected: that connection cannot be used to read the metadata that
   * defines it.
   */
  public static void install(IVariables variables) throws HopException {
    String configured = HopConfig.readOptionString(CONFIG_KEY, "");
    if (StringUtils.isEmpty(StringUtils.trim(configured))) {
      HopMetadataInstance.setSharedMetadataProvider(null);
      return;
    }
    String resolved = resolveFolder(variables, configured);
    checkLocalFolder(resolved);
    SharedMetadataProvider provider = new SharedMetadataProvider(variables, resolved, configured);
    HopMetadataInstance.setSharedMetadataProvider(provider);
    if (HopLogStore.isInitialized()) {
      LogChannel.GENERAL.logBasic("Shared metadata loaded from '" + resolved + "'");
    }
  }

  public static String resolveFolder(IVariables variables, String configured) {
    IVariables space = variables == null ? Variables.getADefaultVariableSpace() : variables;
    String value = configured == null ? "" : configured;
    String configFolder = space.getVariable(VARIABLE);
    if (StringUtils.isEmpty(configFolder)) {
      configFolder = Const.HOP_CONFIG_FOLDER;
    }
    if (StringUtils.isNotEmpty(configFolder)) {
      value = value.replace("${" + VARIABLE + "}", configFolder);
    }
    return space.resolve(value);
  }

  /**
   * @param resolved the folder after variables have been resolved
   * @throws HopException when the path uses a scheme other than {@code file}
   */
  public static void checkLocalFolder(String resolved) throws HopException {
    if (StringUtils.isEmpty(resolved)) {
      throw new HopException("The shared metadata folder is empty");
    }
    int schemeSeparator = resolved.indexOf("://");
    if (schemeSeparator <= 0) {
      return;
    }
    String scheme = resolved.substring(0, schemeSeparator);
    if ("file".equalsIgnoreCase(scheme)) {
      return;
    }
    throw new HopException(
        "Shared metadata folder '"
            + resolved
            + "' is not a local folder. It is read before named VFS connections exist, so it has"
            + " to be a local path.");
  }
}
