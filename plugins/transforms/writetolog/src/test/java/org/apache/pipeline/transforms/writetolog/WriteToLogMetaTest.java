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
package org.apache.pipeline.transforms.writetolog;


import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.pipeline.transforms.writetolog.WriteToLogMeta;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WriteToLogMetaTest implements IInitializer<WriteToLogMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  LoadSaveTester loadSaveTester;
  Class<WriteToLogMetaSymmetric> testMetaClass = WriteToLogMetaSymmetric.class;

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "displayHeader", "limitRows", "limitRowsNumber", "logmessage", "loglevel", "fieldName" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "displayHeader", "isdisplayHeader" );
        put( "limitRows", "isLimitRows" );
        put( "limitRowsNumber", "getLimitRowsNumber" );
        put( "logmessage", "getLogMessage" );
        put( "loglevel", "getLogLevelString" );
        put( "fieldName", "getFieldName" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "displayHeader", "setdisplayHeader" );
        put( "limitRows", "setLimitRows" );
        put( "limitRowsNumber", "setLimitRowsNumber" );
        put( "logmessage", "setLogMessage" );
        put( "loglevel", "setLogLevelString" );
        put( "fieldName", "setFieldName" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fieldName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "loglevel", new LogLevelLoadSaveValidator() );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( WriteToLogMeta someMeta ) {
    if ( someMeta instanceof WriteToLogMeta ) {
      ( (WriteToLogMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class LogLevelLoadSaveValidator implements IFieldLoadSaveValidator<String> {
    final Random rand = new Random();

    @Override
    public String getTestObject() {
      int idx = rand.nextInt( ( WriteToLogMeta.logLevelCodes.length ) );
      return WriteToLogMeta.logLevelCodes[ idx ];
    }

    @Override
    public boolean validateTestObject( String testObject, Object actual ) {
      if ( !( actual instanceof String ) ) {
        return false;
      }
      String actualInput = (String) actual;
      return ( testObject.equals( actualInput ) );
    }
  }
}
