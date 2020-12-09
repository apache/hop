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

package org.apache.hop.pipeline.transforms.cubeoutput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CubeOutputMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes =
      Arrays.asList( "name", "add_to_result_filenames", "do_not_open_newfile_init" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "name", "getFilename" );
    getterMap.put( "add_to_result_filenames", "isAddToResultFiles" );
    getterMap.put( "do_not_open_newfile_init", "isDoNotOpenNewFileInit" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "name", "setFilename" );
    setterMap.put( "add_to_result_filenames", "setAddToResultFiles" );
    setterMap.put( "do_not_open_newfile_init", "setDoNotOpenNewFileInit" );

    LoadSaveTester loadSaveTester =
      new LoadSaveTester( CubeOutputMeta.class, attributes, getterMap, setterMap,
        new HashMap<>(), new HashMap<>() );

    loadSaveTester.testSerialization();
  }
}
