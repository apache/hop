/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.pipeline.transforms.transformsmetrics;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransformsMetricsMetaTest implements InitializerInterface<TransformMetaInterface> {
  LoadSaveTester loadSaveTester;
  Class<TransformsMetricsMeta> testMetaClass = TransformsMetricsMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "transformName", "transformCopyNr", "transformRequired", "transformnamefield", "transformidfield", "transformlinesinputfield",
        "transformlinesoutputfield", "transformlinesreadfield", "transformlinesupdatedfield", "transformlineswrittentfield",
        "transformlineserrorsfield", "transformsecondsfield" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "transformName", "getTransformName" );
        put( "transformCopyNr", "getTransformCopyNr" );
        put( "transformRequired", "getTransformRequired" );
        put( "transformnamefield", "getTransformNameFieldName" );
        put( "transformidfield", "getTransformIdFieldName" );
        put( "transformlinesinputfield", "getTransformLinesInputFieldName" );
        put( "transformlinesoutputfield", "getTransformLinesOutputFieldName" );
        put( "transformlinesreadfield", "getTransformLinesReadFieldName" );
        put( "transformlinesupdatedfield", "getTransformLinesUpdatedFieldName" );
        put( "transformlineswrittentfield", "getTransformLinesWrittenFieldName" );
        put( "transformlineserrorsfield", "getTransformLinesErrorsFieldName" );
        put( "transformsecondsfield", "getTransformSecondsFieldName" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "transformName", "setTransformName" );
        put( "transformCopyNr", "setTransformCopyNr" );
        put( "transformRequired", "setTransformRequired" );
        put( "transformnamefield", "setTransformNameFieldName" );
        put( "transformidfield", "setTransformIdFieldName" );
        put( "transformlinesinputfield", "setTransformLinesInputFieldName" );
        put( "transformlinesoutputfield", "setTransformLinesOutputFieldName" );
        put( "transformlinesreadfield", "setTransformLinesReadFieldName" );
        put( "transformlinesupdatedfield", "setTransformLinesUpdatedFieldName" );
        put( "transformlineswrittentfield", "setTransformLinesWrittenFieldName" );
        put( "transformlineserrorsfield", "setTransformLinesErrorsFieldName" );
        put( "transformsecondsfield", "setTransformSecondsFieldName" );
      }
    };
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 5 );


    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "transformName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "transformCopyNr", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "transformRequired", stringArrayLoadSaveValidator );
    // attrValidatorMap.put( "setEmptyString",
    //     new PrimitiveBooleanArrayLoadSaveValidator( new BooleanLoadSaveValidator(), 5 ) );


    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester = new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  public void modify( TransformMetaInterface someMeta ) {
    if ( someMeta instanceof TransformsMetricsMeta ) {
      ( (TransformsMetricsMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }
}
