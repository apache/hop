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
 *
 */

package org.apache.hop.pipeline.transforms.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import org.apache.hop.core.exception.HopException;

public class ScriptUtils {

  private static ScriptEngineManager scriptEngineManager;

  private static final Map<String, List<ScriptEngineFactory>> languageFactoryMap = new HashMap<>();
  private static List<ScriptEngineFactory> engineFactories = null;

  /**
   * Instantiates the right scripting language interpreter, falling back to Groovy for backward
   * compatibility
   *
   * @param engineName
   * @return the desired ScriptEngine, or null if none can be found
   */
  public static ScriptEngine createNewScriptEngine(String engineName) {

    ScriptEngine scriptEngine = getScriptEngineManager().getEngineByName(engineName);
    if (scriptEngine == null) {
      // falls back to Groovy
      scriptEngine = getScriptEngineManager().getEngineByName("groovy");
    }
    return scriptEngine;
  }

  public static ScriptEngine createNewScriptEngineByLanguage(String languageName)
      throws HopException {
    ScriptEngine scriptEngine = null;

    if (engineFactories == null) {
      populateEngineFactoryMap();
    }
    List<ScriptEngineFactory> factories = languageFactoryMap.get(languageName);
    if (factories != null) {
      for (ScriptEngineFactory factory : factories) {
        try {
          scriptEngine = factory.getScriptEngine();

          if (scriptEngine != null) {
            break;
          }
        } catch (Exception e) {
          throw new HopException(
              "Error getting scripting engine '"
                  + factory.getEngineName()
                  + "' for language '"
                  + languageName
                  + "'",
              e);
        }
      }
    }
    if (scriptEngine == null) {
      throw new HopException("Unable to find script engine for language '" + languageName + "'");
    }
    return scriptEngine;
  }

  public static ScriptEngineManager getScriptEngineManager() {
    if (scriptEngineManager == null) {
      System.setProperty(
          "org.jruby.embed.localvariable.behavior",
          "persistent"); // required for JRuby, transparent
      // for others
      scriptEngineManager = new ScriptEngineManager(ScriptUtils.class.getClassLoader());
      populateEngineFactoryMap();
    }
    return scriptEngineManager;
  }

  public static List<String> getScriptLanguageNames() {
    List<String> scriptEngineNames = new ArrayList<String>();
    engineFactories = getScriptEngineManager().getEngineFactories();
    if (engineFactories != null) {
      for (ScriptEngineFactory factory : engineFactories) {
        final String engineName = factory.getLanguageName();
        scriptEngineNames.add(engineName);
      }
    }
    return scriptEngineNames;
  }

  private static void populateEngineFactoryMap() {
    engineFactories = getScriptEngineManager().getEngineFactories();
    if (engineFactories != null) {
      for (ScriptEngineFactory factory : engineFactories) {
        final String languageName = factory.getLanguageName();
        List<ScriptEngineFactory> languageFactories =
            languageFactoryMap.computeIfAbsent(languageName, k -> new ArrayList<>());
        languageFactories.add(factory);
      }
    }
  }
}
