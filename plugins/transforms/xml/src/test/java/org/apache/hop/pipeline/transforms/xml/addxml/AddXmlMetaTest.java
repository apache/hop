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

package org.apache.hop.pipeline.transforms.xml.addxml;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.Test;

public class AddXmlMetaTest {

  @Test
  public void loadSaveTest() throws HopException {
    List<String> attributes =
        Arrays.asList( "omitXMLheader", "omitNullValues", "encoding", "valueName", "rootNode", "outputFields" );

    XmlField xmlField = new XmlField();
    xmlField.setFieldName( "TEST_FIELD" );
    xmlField.setType( 0 );

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap =
      new HashMap<>();
    fieldLoadSaveValidatorTypeMap.put( XmlField[].class.getCanonicalName(), new ArrayLoadSaveValidator<>(
        new XMLFieldLoadSaveValidator( xmlField ), 1 ) );

//    TransformLoadSaveTester tester = new TransformLoadSaveTester( AddXmlMeta.class, attributes, new HashMap<String, String>(), new HashMap<String, String>(),
//        new HashMap<String, IFieldLoadSaveValidator<?>>(), fieldLoadSaveValidatorTypeMap );

//    tester.testXmlRoundTrip();
  }

  public static class XMLFieldLoadSaveValidator implements IFieldLoadSaveValidator<XmlField> {

    private final XmlField defaultValue;

    public XMLFieldLoadSaveValidator( XmlField defaultValue ) {
      this.defaultValue = defaultValue;
    }

    @Override
    public XmlField getTestObject() {
      return defaultValue;
    }

    @Override
    public boolean validateTestObject(XmlField testObject, Object actual ) {
      return EqualsBuilder.reflectionEquals( testObject, actual );
    }
  }
}
