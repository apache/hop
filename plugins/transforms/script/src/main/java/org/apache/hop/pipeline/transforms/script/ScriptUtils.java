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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import org.apache.hop.core.exception.HopException;

public class ScriptUtils {

  private static ScriptEngineManager scriptEngineManager;

  private static ScriptUtils instance;
  private static final Map<String, List<ScriptEngineFactory>> languageFactoryMap =
      new ConcurrentSkipListMap<>();
  private static List<ScriptEngineFactory> engineFactories = null;

  private ScriptUtils() {
    scriptEngineManager = getScriptEngineManager();
    List<String> scriptLanguageNames = getScriptLanguageNames();
    for (String scriptLanguageName : scriptLanguageNames) {
      createNewScriptEngine(scriptLanguageName);
    }

    populateEngineFactoryMap();
  }

  public static ScriptUtils getInstance() {
    if (instance == null) {
      instance = new ScriptUtils();
    }
    return instance;
  }

  /**
   * Instantiates the right scripting language interpreter, falling back to Groovy for backward
   * compatibility
   *
   * @param engineName
   * @return the desired ScriptEngine, or null if none can be found
   */
  private static ScriptEngine createNewScriptEngine(String engineName) {

    ScriptEngine scriptEngine = getScriptEngineManager().getEngineByName(engineName);
    if (scriptEngine == null) {
      // falls back to Groovy
      scriptEngine = scriptEngineManager.getEngineByName("groovy");
    }
    return scriptEngine;
  }

  private static ScriptEngine createNewScriptEngineByLanguage(String languageName)
      throws HopException {
    ScriptEngine scriptEngine = null;

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

  private static ScriptEngineManager getScriptEngineManager() {
    if (scriptEngineManager == null) {
      System.setProperty(
          "org.jruby.embed.localvariable.behavior",
          "persistent"); // required for JRuby, transparent
      System.setProperty("nashorn.args", "--language=es6");
      // for others
      scriptEngineManager = new ScriptEngineManager(ScriptUtils.class.getClassLoader());
    }
    return scriptEngineManager;
  }

  public static List<String> getScriptLanguageNames() {
    List<String> scriptEngineNames = new ArrayList<>();
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

  public ScriptEngine getScriptEngineByName(String scriptLanguegeName) {
    return scriptEngineManager.getEngineByName(scriptLanguegeName);
  }
}
