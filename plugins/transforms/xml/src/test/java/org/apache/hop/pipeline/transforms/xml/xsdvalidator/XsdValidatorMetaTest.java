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

package org.apache.hop.pipeline.transforms.xml.xsdvalidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hop.core.exception.HopException;
import org.junit.Test;

public class XsdValidatorMetaTest {

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes =
        Arrays.asList( "xdsfilename", "xmlstream", "resultfieldname", "addvalidationmsg", "validationmsgfield",
            "ifxmlunvalid", "ifxmlvalid", "outputstringfield", "xmlsourcefile", "xsddefinedfield", "xsdsource" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "xdsfilename", "getXSDFilename" );
    getterMap.put( "xmlstream", "getXMLStream" );
    getterMap.put( "resultfieldname", "getResultfieldname" );
    getterMap.put( "addvalidationmsg", "useAddValidationMessage" );
    getterMap.put( "validationmsgfield", "getValidationMessageField" );
    getterMap.put( "ifxmlunvalid", "getIfXmlInvalid" );
    getterMap.put( "ifxmlvalid", "getIfXmlValid" );
    getterMap.put( "outputstringfield", "getOutputStringField" );
    getterMap.put( "xmlsourcefile", "getXMLSourceFile" );
    getterMap.put( "xsddefinedfield", "getXSDDefinedField" );
    getterMap.put( "xsdsource", "getXSDSource" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "xdsfilename", "setXSDfilename" );
    setterMap.put( "xmlstream", "setXMLStream" );
    setterMap.put( "resultfieldname", "setResultfieldname" );
    setterMap.put( "addvalidationmsg", "setAddValidationMessage" );
    setterMap.put( "validationmsgfield", "setValidationMessageField" );
    setterMap.put( "ifxmlunvalid", "setIfXmlInvalid" );
    setterMap.put( "ifxmlvalid", "setIfXMLValid" );
    setterMap.put( "outputstringfield", "setOutputStringField" );
    setterMap.put( "xmlsourcefile", "setXMLSourceFile" );
    setterMap.put( "xsddefinedfield", "setXSDDefinedField" );
    setterMap.put( "xsdsource", "setXSDSource" );

//    TransformLoadSaveTester loadSaveTester =
//        new TransformLoadSaveTester( XsdValidatorMeta.class, attributes, getterMap, setterMap,
//            new HashMap<String, IFieldLoadSaveValidator<?>>(), new HashMap<String, IFieldLoadSaveValidator<?>>() );
//
//    loadSaveTester.testXmlRoundTrip();
  }
}
