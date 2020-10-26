/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.systemdata;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * User: Dzmitry Stsiapanau Date: 1/20/14 Time: 3:04 PM
 */
public class SystemDataMetaTest implements InitializerInterface<ITransform> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  LoadSaveTester loadSaveTester;
  Class<SystemDataMeta> testMetaClass = SystemDataMeta.class;
  SystemDataMeta expectedSystemDataMeta;
  String expectedXML = "    <fields>\n" + "      <field>\n" + "        <name>hostname_real</name>\n"
    + "        <type>Hostname real</type>\n" + "        </field>\n" + "      <field>\n"
    + "        <name>hostname</name>\n" + "        <type>Hostname</type>\n" + "        </field>\n"
    + "      </fields>\n";

  @Before
  public void setUp() throws Exception {
    expectedSystemDataMeta = new SystemDataMeta();
    expectedSystemDataMeta.allocate( 2 );
    String[] names = expectedSystemDataMeta.getFieldName();
    SystemDataTypes[] types = expectedSystemDataMeta.getFieldType();
    names[ 0 ] = "hostname_real";
    names[ 1 ] = "hostname";
    types[ 0 ] = SystemDataTypes.getTypeFromString( SystemDataTypes.TYPE_SYSTEM_INFO_HOSTNAME_REAL.getDescription() );
    types[ 1 ] = SystemDataTypes.getTypeFromString( SystemDataTypes.TYPE_SYSTEM_INFO_HOSTNAME.getDescription() );
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testLoadXML() throws Exception {
    SystemDataMeta systemDataMeta = new SystemDataMeta();
    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    Document document = documentBuilder.parse( new InputSource( new StringReader( expectedXML ) ) );
    Node node = document;
    IHopMetadataProvider store = null;
    systemDataMeta.loadXml( node, store );
    assertEquals( expectedSystemDataMeta, systemDataMeta );
  }

  @Test
  public void testGetXML() throws Exception {
    String generatedXML = expectedSystemDataMeta.getXml();
    assertEquals( expectedXML.replaceAll( "\n", "" ).replaceAll( "\r", "" ), generatedXML.replaceAll( "\n", "" )
      .replaceAll( "\r", "" ) );
  }

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "fieldName", "fieldType" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "fieldName", "getFieldName" );
        put( "fieldType", "getFieldType" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "fieldName", "setFieldName" );
        put( "fieldType", "setFieldType" );
      }
    };
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 5 );

    FieldLoadSaveValidator<SystemDataTypes[]> sdtArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<SystemDataTypes>( new SystemDataTypesLoadSaveValidator(), 5 );

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "fieldName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldType", sdtArrayLoadSaveValidator );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof SystemDataMeta ) {
      ( (SystemDataMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class SystemDataTypesLoadSaveValidator implements FieldLoadSaveValidator<SystemDataTypes> {
    final Random rand = new Random();

    @Override
    public SystemDataTypes getTestObject() {
      SystemDataTypes[] allTypes = SystemDataTypes.values();
      return allTypes[ rand.nextInt( allTypes.length ) ];
    }

    @Override
    public boolean validateTestObject( SystemDataTypes testObject, Object actual ) {
      if ( !( actual instanceof SystemDataTypes ) ) {
        return false;
      }
      SystemDataTypes actualInput = (SystemDataTypes) actual;
      return ( testObject.toString().equals( actualInput.toString() ) );
    }
  }


}
