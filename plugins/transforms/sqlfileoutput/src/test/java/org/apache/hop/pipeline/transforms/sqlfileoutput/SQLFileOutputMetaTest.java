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
package org.apache.hop.pipeline.transforms.sqlfileoutput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLFileOutputMetaTest {
  LoadSaveTester loadSaveTester;
  Class<SQLFileOutputMeta> testMetaClass = SQLFileOutputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "databaseMeta", "schemaName", "tablename", "truncateTable", "AddToResult", "createTable", "fileName",
        "extension", "splitEvery", "fileAppended", "transformNrInFilename", "dateInFilename", "timeInFilename",
        "encoding", "dateFormat", "StartNewLine", "createParentFolder", "DoNotOpenNewFileInit" );

    // Note - "partNrInFilename" is used in serialization/deserialization, but there is no getter/setter for it and it's
    // not present in the dialog. Looks like a copy/paste thing, and the value itself will end up serialized/deserialized
    // as false.
    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "truncateTable", "truncateTable" );
        put( "AddToResult", "AddToResult" );
        put( "createTable", "createTable" );
        put( "StartNewLine", "StartNewLine" );
      }
    };
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

}
