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

package org.apache.hop.server.loadbalance;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.OutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/** Writes assignment ledger files under an optional VFS state folder. */
public class LoadBalancingAssignmentStore {

  private final IVariables variables;
  private final ILogChannel log;
  private final ObjectMapper mapper = HopJson.newMapper();

  public LoadBalancingAssignmentStore(IVariables variables, ILogChannel log) {
    this.variables = variables;
    this.log = log;
  }

  public void save(String stateFolder, LoadBalancingAssignment assignment) {
    if (assignment == null || StringUtils.isEmpty(stateFolder)) {
      return;
    }
    String resolvedFolder = variables.resolve(stateFolder);
    if (StringUtils.isEmpty(resolvedFolder) || StringUtils.isEmpty(assignment.getExecutionId())) {
      return;
    }
    String runConfig = sanitize(assignment.getRunConfigurationName());
    String executionId = sanitize(assignment.getExecutionId());
    String path = resolvedFolder + "/" + runConfig + "/assignments/" + executionId + ".json";
    try {
      FileObject file = HopVfs.getFileObject(path, variables);
      FileObject parent = file.getParent();
      if (parent != null && !parent.exists()) {
        parent.createFolder();
      }
      try (OutputStream out = HopVfs.getOutputStream(file, false)) {
        out.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(assignment));
        out.write('\n');
      }
    } catch (Exception e) {
      if (log != null) {
        log.logError("Unable to write load-balancing assignment file '" + path + "'", e);
      }
    }
  }

  public LoadBalancingAssignment load(
      String stateFolder, String runConfigurationName, String executionId) throws HopException {
    if (StringUtils.isEmpty(stateFolder) || StringUtils.isEmpty(executionId)) {
      return null;
    }
    String resolvedFolder = variables.resolve(stateFolder);
    String path =
        resolvedFolder
            + "/"
            + sanitize(runConfigurationName)
            + "/assignments/"
            + sanitize(executionId)
            + ".json";
    try {
      FileObject file = HopVfs.getFileObject(path, variables);
      if (!file.exists()) {
        return null;
      }
      try (var in = HopVfs.getInputStream(file)) {
        return mapper.readValue(in, LoadBalancingAssignment.class);
      }
    } catch (Exception e) {
      throw new HopException("Unable to read load-balancing assignment file '" + path + "'", e);
    }
  }

  static String sanitize(String name) {
    if (StringUtils.isEmpty(name)) {
      return "unnamed";
    }
    return name.replaceAll("[^A-Za-z0-9._-]", "_");
  }
}
