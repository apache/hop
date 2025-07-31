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

package org.apache.hop.core.variables;

import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/** This class is an implementation of IVariables */
public class Variables implements IVariables {
  private Map<String, String> properties;

  private IVariables parent;

  private Map<String, String> injection;

  private boolean initialized;

  public Variables() {
    properties = Collections.synchronizedMap(new HashMap<>());
    parent = null;
    injection = null;
    initialized = false;
  }

  @Override
  public void copyFrom(IVariables variables) {
    if (variables != null && this != variables) {
      // If variables is not null and this variable is not already
      // the same object as the argument.
      String[] variableNames = variables.getVariableNames();
      for (int idx = 0; idx < variableNames.length; idx++) {
        properties.put(variableNames[idx], variables.getVariable(variableNames[idx]));
      }
    }
  }

  @Override
  public IVariables getParentVariables() {
    return parent;
  }

  @Override
  public void setParentVariables(IVariables parent) {
    this.parent = parent;
  }

  @Override
  public String getVariable(String variableName, String defaultValue) {
    String value = properties.get(variableName);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  @Override
  public String getVariable(String variableName) {
    return properties.get(variableName);
  }

  @Override
  public boolean getVariableBoolean(String variableName, boolean defaultValue) {
    if (!Utils.isEmpty(variableName)) {
      String value = properties.get(variableName);
      if (!Utils.isEmpty(value)) {
        return ValueMetaBase.convertStringToBoolean(value);
      }
    }
    return defaultValue;
  }

  @Override
  public void initializeFrom(IVariables parent) {
    this.parent = parent;

    // Clone the system properties to avoid ConcurrentModificationException while iterating
    // and then add all of them to properties variable.
    //
    Set<String> systemPropertiesNames = System.getProperties().stringPropertyNames();
    for (String key : systemPropertiesNames) {
      getProperties().put(key, System.getProperties().getProperty(key));
    }

    List<DescribedVariable> describedVariables = HopConfig.getInstance().getDescribedVariables();
    for (DescribedVariable describedVariable : describedVariables) {
      getProperties().put(describedVariable.getName(), describedVariable.getValue());
    }

    if (parent != null) {
      copyFrom(parent);
    }
    if (injection != null) {
      getProperties().putAll(injection);
      injection = null;
    }
    initialized = true;
  }

  @Override
  public String[] getVariableNames() {
    Set<String> keySet = properties.keySet();
    return keySet.toArray(new String[0]);
  }

  @Override
  public synchronized void setVariable(String variableName, String variableValue) {
    if (variableValue != null) {
      properties.put(variableName, variableValue);
    } else {
      properties.remove(variableName);
    }
  }

  @Override
  public synchronized String resolve(String aString) {
    if (Utils.isEmpty(aString)) {
      return aString;
    }

    String resolved = StringUtil.environmentSubstitute(aString, properties);

    String r = substituteVariableResolvers(resolved);
    if (r != null) {
      resolved = r;
    }

    return resolved;
  }

  private String substituteVariableResolvers(String input) {
    String resolved = input;
    int startIndex = 0;
    while (startIndex < resolved.length()) {
      int resolverIndex = resolved.indexOf(StringUtil.RESOLVER_OPEN);
      if (resolverIndex < 0) {
        // There's nothing more to do here.
        //
        return null;
      }

      // Is there a close token?
      //
      int closeIndex = resolved.indexOf(StringUtil.RESOLVER_CLOSE, resolverIndex);
      if (closeIndex < 0) {
        // False positive on the open token
        //
        return null;
      }

      /*
       The following variable resolver string

          [resolverIndex, closeIndex$]

       is in the format:

         #{name:arguments}

      */
      int colonIndex = resolved.indexOf(':', resolverIndex);
      String name;
      String arguments;
      if (colonIndex >= 0) {
        name = resolved.substring(resolverIndex + StringUtil.RESOLVER_OPEN.length(), colonIndex);
        arguments = resolved.substring(colonIndex + 1, closeIndex);
      } else {
        name = resolved.substring(resolverIndex + StringUtil.RESOLVER_OPEN.length(), closeIndex);
        arguments = "";
      }

      try {
        MultiMetadataProvider provider = HopMetadataInstance.getMetadataProvider();
        IHopMetadataSerializer<VariableResolver> serializer =
            provider.getSerializer(VariableResolver.class);

        // This next loadA() method is relatively slow since it needs to go to disk.
        //
        VariableResolver resolver = serializer.load(name);
        if (resolver == null) {
          if (LogChannel.GENERAL.isDetailed()) {
            LogChannel.GENERAL.logDetailed(
                "WARNING: Variable Resolver '" + name + "' could not be found in the metadata");
          }
          return resolved;
        }

        /*

         The arguments come in the form: path-key:value-key
         For example: #{vault:hop/data/some-db:hostname}
         The secret path part will be "hop/data/some-db" and the secret value part is "hostname".

        */
        String secretPath;
        String secretValue = null;

        String[] parameters = arguments.split(":");
        secretPath = parameters[0];
        if (parameters.length > 1) {
          secretValue = parameters[1];
        }

        String resolvedArgument = resolver.getIResolver().resolve(secretPath, this);
        if (StringUtils.isEmpty(resolvedArgument)) {
          return resolved;
        }

        // If we have a value to retrieve from the JSON we got back, we can do that:
        //
        if (StringUtils.isEmpty(secretValue)) {
          return resolvedArgument;
        } else {
          try {
            JSONObject js = (JSONObject) new JSONParser().parse(resolvedArgument);
            Object value = js.get(secretValue);
            if (value == null) {
              // Value not found in the secret
              resolvedArgument = "";
            } else {
              resolvedArgument = value.toString();
            }
          } catch (Exception e) {
            LogChannel.GENERAL.logError(
                "Error parsing JSON '"
                    + resolvedArgument
                    + "} to retrieve secret value '"
                    + secretValue
                    + "'",
                e);
            // Keep the origin String
          }
        }

        // We take the first part of the original resolved string, the resolved arguments, and the
        // last part
        // to continue resolving all expressions in the string.
        //
        String before = resolved.substring(0, resolverIndex);
        String after = resolved.substring(closeIndex + 1);
        resolved = before + resolvedArgument + after;

        // Where do we continue?
        startIndex = before.length() + resolvedArgument.length();
      } catch (HopException e) {
        throw new RuntimeException(
            "Error resolving variable '" + input + "' with variable resolver metadata", e);
      }
    }
    return resolved;
  }

  /**
   * Substitutes field values in <code>aString</code>. Field values are of the form {@literal
   * "?{<fieldname>}"} . The values are retrieved from the specified row. Please note that the
   * getString() method is used to convert to a String, for all values in the row.
   *
   * @param aString the string on which to apply the substitution.
   * @param rowMeta The row metadata to use.
   * @param rowData The row data to use
   * @return the string with the substitution applied.
   * @throws HopValueException In case there is a String conversion error
   */
  @Override
  public String resolve(String aString, IRowMeta rowMeta, Object[] rowData)
      throws HopValueException {
    if (Utils.isEmpty(aString)) {
      return aString;
    }

    return StringUtil.substituteField(aString, rowMeta, rowData);
  }

  @Override
  public String[] resolve(String[] string) {
    String[] retval = new String[string.length];
    for (int i = 0; i < string.length; i++) {
      retval[i] = resolve(string[i]);
    }
    return retval;
  }

  @Override
  public void shareWith(IVariables variables) {
    // not implemented in here... done by pointing to the same IVariables
    // implementation
  }

  @Override
  public void setVariables(Map<String, String> map) {
    if (initialized) {
      // variables are already initialized
      if (map != null) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
          if (!Utils.isEmpty(entry.getKey())) {
            properties.put(entry.getKey(), Const.NVL(entry.getValue(), ""));
          }
        }
        injection = null;
      }
    } else {
      // We have our own personal copy, so changes afterwards
      // to the input properties don't affect us.
      injection = new Hashtable<>();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        if (!Utils.isEmpty(entry.getKey())) {
          injection.put(entry.getKey(), Const.NVL(entry.getValue(), ""));
        }
      }
    }
  }

  /**
   * Get a default variable variables as a placeholder. Every time you will get a new instance.
   *
   * @return a default variable variables.
   */
  public static synchronized IVariables getADefaultVariableSpace() {
    IVariables variables = new Variables();

    variables.initializeFrom(null);

    return variables;
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  Map<String, String> getProperties() {
    return properties;
  }
}
