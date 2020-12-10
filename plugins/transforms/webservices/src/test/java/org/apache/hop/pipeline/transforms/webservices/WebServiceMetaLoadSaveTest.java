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
package org.apache.hop.pipeline.transforms.webservices;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.webservices.wsdl.XsdType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class WebServiceMetaLoadSaveTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  LoadSaveTester loadSaveTester;
  Class<WebServiceMeta> testMetaClass = WebServiceMeta.class;

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "url", "operationName", "operationRequestName", "operationNamespace", "inFieldContainerName",
        "inFieldArgumentName", "outFieldContainerName", "outFieldArgumentName", "proxyHost", "proxyPort", "httpLogin",
        "httpPassword", "passingInputData", "callTransform", "compatible", "repeatingElementName", "returningReplyAsString",
        "fieldsIn", "fieldsOut" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fieldsIn",
      new ListLoadSaveValidator<>( new WebServiceFieldLoadSaveValidator(), 5 ) );
    attrValidatorMap.put( "fieldsOut",
      new ListLoadSaveValidator<>( new WebServiceFieldLoadSaveValidator(), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class WebServiceFieldLoadSaveValidator implements IFieldLoadSaveValidator<WebServiceField> {
    final Random rand = new Random();

    @Override
    public WebServiceField getTestObject() {
      WebServiceField rtn = new WebServiceField();
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setWsName( UUID.randomUUID().toString() );
      rtn.setXsdType( XsdType.TYPES[ rand.nextInt( XsdType.TYPES.length ) ] );
      return rtn;
    }

    @Override
    public boolean validateTestObject( WebServiceField testObject, Object actual ) {
      if ( !( actual instanceof WebServiceField ) ) {
        return false;
      }
      WebServiceField another = (WebServiceField) actual;
      return new EqualsBuilder()
        .append( testObject.getName(), another.getName() )
        .append( testObject.getWsName(), another.getWsName() )
        .append( testObject.getXsdType(), another.getXsdType() )
        .isEquals();
    }
  }

}
