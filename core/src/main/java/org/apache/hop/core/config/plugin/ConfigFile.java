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

package org.apache.hop.core.config.plugin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.ConfigFileSerializer;
import org.apache.hop.core.config.ConfigNoFileSerializer;
import org.apache.hop.core.config.IConfigFile;
import org.apache.hop.core.config.IHopConfigSerializer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.vfs.HopVfs;

public abstract class ConfigFile implements IConfigFile {

  public static final String HOP_VARIABLES_KEY = "variables";
  public static final String HOP_CONFIG_KEY = "config";

  /**
   * Held while the configuration is written, and by the callers that change what is written.
   *
   * <p>Hop Web serves many people from one JVM and they share this configuration: two of them
   * closing a dialog at the same moment had both writing the file at once, which failed the save
   * outright and reported it to whoever happened to be second. Shared by every configuration file
   * because there are only a handful of them and they are written rarely.
   *
   * <p>It does not cover a caller that changes the map it got from {@link #getConfigMap()} without
   * asking for it: {@link #getDescribedVariables()} is one, and it deliberately stays out because
   * it runs for every log message. What it does cover is the writing itself, which is where the
   * damage was.
   */
  protected static final Object CONFIG_LOCK = new Object();

  @Getter
  @Setter
  @JsonProperty("config")
  protected Map<String, Object> configMap;

  @Getter @Setter @JsonIgnore protected IHopConfigSerializer serializer;
  @Setter @JsonIgnore protected boolean inMemory;

  public boolean isInMemory() {
    return inMemory
        || "Y".equalsIgnoreCase(System.getProperty(Const.HOP_CONFIG_IN_MEMORY, "N"))
        || "true".equalsIgnoreCase(System.getProperty(Const.HOP_CONFIG_IN_MEMORY, "false"));
  }

  public ConfigFile() {
    configMap = new HashMap<>();
    serializer = new ConfigNoFileSerializer();
  }

  public void readFromFile() throws HopException {
    try {
      boolean inMemoryMode = isInMemory();
      boolean exists;
      try (FileObject configFile = HopVfs.getFileObject(getConfigFilename())) {
        exists = configFile.exists();
      }
      if (inMemoryMode) {
        this.serializer = new ConfigNoFileSerializer();
      } else if (exists) {
        // Let's write to the file
        //
        this.serializer = new ConfigFileSerializer();
      } else {
        boolean createWhenMissing =
            "Y".equalsIgnoreCase(System.getProperty(Const.HOP_AUTO_CREATE_CONFIG, "N"));
        if (createWhenMissing) {
          System.out.println("Creating new default Hop configuration file: " + getConfigFilename());
          this.serializer = new ConfigFileSerializer();
        } else {
          // Doesn't serialize anything really, reads an empty map with an empty file
          //
          System.out.println(
              "Hop configuration file not found, not serializing: " + getConfigFilename());
          this.serializer = new ConfigNoFileSerializer();
        }
      }
      if (inMemoryMode && exists) {
        configMap = new ConfigFileSerializer().readFromFile(getConfigFilename());
      } else {
        configMap = serializer.readFromFile(getConfigFilename());
      }
    } catch (Exception e) {
      throw new HopException("Unable to read config file '" + getConfigFilename() + "'", e);
    }
  }

  public void saveToFile() throws HopException {
    if (isInMemory()) {
      return;
    }
    synchronized (CONFIG_LOCK) {
      try {
        serializer.writeToFile(getConfigFilename(), configMap);
      } catch (Exception e) {
        throw new HopException("Error saving configuration file '" + getConfigFilename() + "'", e);
      }
    }
  }

  public ConfigFile(String filename, List<DescribedVariable> describedVariables) {
    this();
    setConfigFilename(filename);
    setDescribedVariables(describedVariables);
  }

  @JsonIgnore
  @Override
  public List<DescribedVariable> getDescribedVariables() {
    List<DescribedVariable> variables = new ArrayList<>();

    Map<String, Object> configObj = (Map<String, Object>) configMap.get(HOP_CONFIG_KEY);
    if (configObj != null) {
      configMap = configObj;
    }

    Object variablesObject = configMap.get(HOP_VARIABLES_KEY);

    // The list is stored back below, so from the second call onwards it already holds
    // DescribedVariable objects. Serialising those to JSON only to parse them straight back would
    // be pure overhead, and this method sits on the path of every single log message.
    //
    if (variablesObject instanceof List<?> describedList
        && (describedList.isEmpty() || describedList.get(0) instanceof DescribedVariable)) {
      return (List<DescribedVariable>) variablesObject;
    }

    if (variablesObject != null) {
      try {
        for (Object dvObject : (List) variablesObject) {
          String dvJson = new Gson().toJson(dvObject);
          DescribedVariable describedVariable =
              HopJson.newMapper().readValue(dvJson, DescribedVariable.class);
          variables.add(describedVariable);
        }
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error parsing described variables from configuration file '"
                + getConfigFilename()
                + "'",
            e);
        variables = new ArrayList<>();
      }
    }

    configMap.put(HOP_VARIABLES_KEY, variables);

    return variables;
  }

  @Override
  public DescribedVariable findDescribedVariable(String name) {
    for (DescribedVariable describedVariable : getDescribedVariables()) {
      if (describedVariable.getName().equals(name)) {
        return describedVariable;
      }
    }
    return null;
  }

  @Override
  public void setDescribedVariable(DescribedVariable variable) {
    for (DescribedVariable describedVariable : getDescribedVariables()) {
      if (describedVariable.getName().equals(variable.getName())) {
        // Variable found? Update the value and description
        //
        describedVariable.setValue(variable.getValue());
        describedVariable.setDescription(variable.getDescription());
        return;
      }
    }
    // Variable not found? Add it
    //
    getDescribedVariables().add(variable);
  }

  @Override
  public String findDescribedVariableValue(String name) {
    DescribedVariable describedVariable = findDescribedVariable(name);
    if (describedVariable == null) {
      return null;
    }
    return describedVariable.getValue();
  }

  @Override
  public void setDescribedVariables(List<DescribedVariable> describedVariables) {
    // Kept mutable: getDescribedVariables() hands this very list out and setDescribedVariable()
    // adds to it.
    configMap.put(
        HOP_VARIABLES_KEY,
        describedVariables == null ? new ArrayList<>() : new ArrayList<>(describedVariables));
  }
}
