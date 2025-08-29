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
package org.apache.hop.pipeline.transforms.writetolog;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.EnumLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class WriteToLogMetaTest implements IInitializer<WriteToLogMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  LoadSaveTester<WriteToLogMeta> loadSaveTester;

  @BeforeEach
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes =
        List.of(
            "DisplayHeader", "LimitRows", "LimitRowsNumber", "LogMessage", "LogLevel", "LogFields");

    Map<String, String> getterMap =
        new HashMap<>() {
          {
            put("DisplayHeader", "isDisplayHeader");
            put("LimitRows", "isLimitRows");
            put("LimitRowsNumber", "getLimitRowsNumber");
            put("LogMessage", "getLogMessage");
            put("LogLevel", "getLogLevel");
            put("LogFields", "getLogFields");
          }
        };

    Map<String, String> setterMap =
        new HashMap<>() {
          {
            put("DisplayHeader", "setDisplayHeader");
            put("LimitRows", "setLimitRows");
            put("LimitRowsNumber", "setLimitRowsNumber");
            put("LogMessage", "setLogMessage");
            put("LogLevel", "setLogLevel");
            put("LogFields", "setLogFields");
          }
        };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put("DisplayHeader", new BooleanLoadSaveValidator());
    attrValidatorMap.put("LimitRows", new BooleanLoadSaveValidator());
    attrValidatorMap.put("LimitRowsNumber", new IntLoadSaveValidator());
    attrValidatorMap.put("LogMessage", new StringLoadSaveValidator());
    attrValidatorMap.put("LogLevel", createLogLevelValidators());
    attrValidatorMap.put("LogFields", new ListLoadSaveValidator<>(new LogFieldLoadSaveValidator()));

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester<WriteToLogMeta>(
            WriteToLogMeta.class,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  protected EnumLoadSaveValidator<LogLevel> createLogLevelValidators() {
    EnumSet<LogLevel> logLevels = EnumSet.allOf(LogLevel.class);
    LogLevel random = (LogLevel) logLevels.toArray()[new Random().nextInt(logLevels.size())];
    return new EnumLoadSaveValidator<>(random);
  }

  private final class LogFieldLoadSaveValidator implements IFieldLoadSaveValidator<LogField> {
    @Override
    public boolean validateTestObject(LogField testObject, Object actual) {
      if (!(actual instanceof LogField)) {
        return false;
      }
      LogField actualInput = (LogField) actual;
      return (testObject.equals(actualInput));
    }

    @Override
    public LogField getTestObject() {
      return new LogField(UUID.randomUUID().toString());
    }
  }

  @Override
  public void modify(WriteToLogMeta object) {
    // TODO Auto-generated method stub

  }
}
