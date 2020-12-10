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

package org.apache.hop.pipeline.transforms.janino;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class JaninoMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopPluginException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes = Arrays.asList( "formula" );

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<>();

    IFieldLoadSaveValidator<JaninoMetaFunction[]> janinoMetaFunctionArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new JaninoMetaFunctionFieldLoadSaveValidator(), 25 );

    fieldLoadSaveValidatorAttributeMap.put( "formula", janinoMetaFunctionArrayLoadSaveValidator );

    LoadSaveTester loadSaveTester =
      new LoadSaveTester( JaninoMeta.class, attributes, new HashMap<>(), new HashMap<>(),
        fieldLoadSaveValidatorAttributeMap, new HashMap<>() );

    loadSaveTester.testSerialization();
  }

  public class JaninoMetaFunctionFieldLoadSaveValidator implements IFieldLoadSaveValidator<JaninoMetaFunction> {
    @Override
    public JaninoMetaFunction getTestObject() {
      Random random = new Random();
      return new JaninoMetaFunction(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        random.nextInt( ValueMetaFactory.getAllValueMetaNames().length ),
        random.nextInt( Integer.MAX_VALUE ),
        random.nextInt( Integer.MAX_VALUE ),
        UUID.randomUUID().toString() );
    }

    @Override
    public boolean validateTestObject( JaninoMetaFunction testObject, Object actual ) {
      return testObject.equals( actual );
    }
  }
}
