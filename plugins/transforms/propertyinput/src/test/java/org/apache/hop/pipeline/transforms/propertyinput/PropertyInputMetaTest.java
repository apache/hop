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
package org.apache.hop.pipeline.transforms.propertyinput;

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.*;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class PropertyInputMetaTest implements IInitializer<ITransformMeta> {
  Class<PropertyInputMeta> testMetaClass = PropertyInputMeta.class;
  LoadSaveTester loadSaveTester;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "encoding", "fileType", "includeFilename", "resetRowNumber", "resolvevaluevariable",
        "filenameField", "includeRowNumber", "rowNumberField", "rowLimit", "filefield", "isaddresult",
        "dynamicFilenameField", "includeIniSection", "iniSectionField", "section", "shortFileFieldName",
        "pathFieldName", "hiddenFieldName", "lastModificationTimeFieldName", "uriNameFieldName", "rootUriNameFieldName",
        "extensionFieldName", "sizeFieldName", "fileName", "fileMask", "excludeFileMask", "fileRequired",
        "includeSubFolders", "inputFields" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "encoding", "getEncoding" );
        put( "fileType", "getFileType" );
        put( "includeFilename", "includeFilename" );
        put( "resetRowNumber", "resetRowNumber" );
        put( "resolvevaluevariable", "isResolveValueVariable" );
        put( "filenameField", "getFilenameField" );
        put( "includeRowNumber", "includeRowNumber" );
        put( "rowNumberField", "getRowNumberField" );
        put( "rowLimit", "getRowLimit" );
        put( "filefield", "isFileField" );
        put( "isaddresult", "isAddResultFile" );
        put( "dynamicFilenameField", "getDynamicFilenameField" );
        put( "includeIniSection", "includeIniSection" );
        put( "iniSectionField", "getINISectionField" );
        put( "section", "getSection" );
        put( "shortFileFieldName", "getShortFileNameField" );
        put( "pathFieldName", "getPathField" );
        put( "hiddenFieldName", "isHiddenField" );
        put( "lastModificationTimeFieldName", "getLastModificationDateField" );
        put( "uriNameFieldName", "getUriField" );
        put( "rootUriNameFieldName", "getRootUriField" );
        put( "extensionFieldName", "getExtensionField" );
        put( "sizeFieldName", "getSizeField" );
        put( "fileName", "getFileName" );
        put( "fileMask", "getFileMask" );
        put( "excludeFileMask", "getExcludeFileMask" );
        put( "fileRequired", "getFileRequired" );
        put( "includeSubFolders", "getIncludeSubFolders" );
        put( "inputFields", "getInputFields" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "encoding", "setEncoding" );
        put( "fileType", "setFileType" );
        put( "includeFilename", "setIncludeFilename" );
        put( "resetRowNumber", "setResetRowNumber" );
        put( "resolvevaluevariable", "setResolveValueVariable" );
        put( "filenameField", "setFilenameField" );
        put( "includeRowNumber", "setIncludeRowNumber" );
        put( "rowNumberField", "setRowNumberField" );
        put( "rowLimit", "setRowLimit" );
        put( "filefield", "setFileField" );
        put( "isaddresult", "setAddResultFile" );
        put( "dynamicFilenameField", "setDynamicFilenameField" );
        put( "includeIniSection", "setIncludeIniSection" );
        put( "iniSectionField", "setINISectionField" );
        put( "section", "setSection" );
        put( "shortFileFieldName", "setShortFileNameField" );
        put( "pathFieldName", "setPathField" );
        put( "hiddenFieldName", "setIsHiddenField" );
        put( "lastModificationTimeFieldName", "setLastModificationDateField" );
        put( "uriNameFieldName", "setUriField" );
        put( "rootUriNameFieldName", "setRootUriField" );
        put( "extensionFieldName", "setExtensionField" );
        put( "sizeFieldName", "setSizeField" );
        put( "fileName", "setFileName" );
        put( "fileMask", "setFileMask" );
        put( "excludeFileMask", "setExcludeFileMask" );
        put( "fileRequired", "setFileRequired" );
        put( "includeSubFolders", "setIncludeSubFolders" );
        put( "inputFields", "setInputFields" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    IFieldLoadSaveValidator<PropertyInputField[]> pifArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new PropertyInputFieldLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fileName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fileMask", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "excludeFileMask", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fileRequired", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "includeSubFolders", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "inputFields", pifArrayLoadSaveValidator );
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();
    // typeValidatorMap.put( int[].class.getCanonicalName(), new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator(), 1 ) );

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  public void modify( ITransformMeta propInputMeta ) {
    if ( propInputMeta instanceof PropertyInputMeta ) {
      ( (PropertyInputMeta) propInputMeta ).allocate( 5, 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  //PropertyInputField
  public class PropertyInputFieldLoadSaveValidator implements IFieldLoadSaveValidator<PropertyInputField> {
    final Random rand = new Random();

    @Override
    public PropertyInputField getTestObject() {
      PropertyInputField rtn = new PropertyInputField();
      rtn.setCurrencySymbol( UUID.randomUUID().toString() );
      rtn.setDecimalSymbol( UUID.randomUUID().toString() );
      rtn.setFormat( UUID.randomUUID().toString() );
      rtn.setGroupSymbol( UUID.randomUUID().toString() );
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setTrimType( rand.nextInt( 4 ) );
      rtn.setPrecision( rand.nextInt( 9 ) );
      rtn.setRepeated( rand.nextBoolean() );
      rtn.setLength( rand.nextInt( 50 ) );
      rtn.setSamples( new String[] { UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString() } );
      return rtn;
    }

    @Override
    public boolean validateTestObject( PropertyInputField testObject, Object actual ) {
      if ( !( actual instanceof PropertyInputField ) ) {
        return false;
      }
      PropertyInputField actualInput = (PropertyInputField) actual;
      return ( testObject.toString().equals( actualInput.toString() ) );
    }
  }

  @Test
  @Ignore
  public void testOpenNextFile() throws Exception {

    PropertyInputMeta propertyInputMeta = Mockito.mock( PropertyInputMeta.class );
    PropertyInputData propertyInputData = new PropertyInputData();
    FileInputList fileInputList = new FileInputList();
    FileObject fileObject = Mockito.mock( FileObject.class );
    FileName fileName = Mockito.mock( FileName.class );
    Mockito.when( fileName.getRootURI() ).thenReturn( "testFolder" );
    Mockito.when( fileName.getURI() ).thenReturn( "testFileName.ini" );

    String header = "test ini data with umlauts";
    String key = "key";
    String testValue = "value-with-äöü";
    String testData = "[" + header + "]\r\n"
      + key + "=" + testValue;
    String charsetEncode = "Windows-1252";

    InputStream inputStream = new ByteArrayInputStream( testData.getBytes(
      Charset.forName( charsetEncode ) ) );
    FileContent fileContent = Mockito.mock( FileContent.class );
    Mockito.when( fileObject.getContent() ).thenReturn( fileContent );
    Mockito.when( fileContent.getInputStream() ).thenReturn( inputStream );
    Mockito.when( fileObject.getName() ).thenReturn( fileName );
    fileInputList.addFile( fileObject );

    propertyInputData.files = fileInputList;
    propertyInputData.propfiles = false;
    propertyInputData.realEncoding = charsetEncode;

    PropertyInput propertyInput = Mockito.mock( PropertyInput.class );

    Field logField = BaseTransform.class.getDeclaredField( "log" );
    logField.setAccessible( true );
    logField.set( propertyInput, Mockito.mock( ILogChannel.class ) );

    Mockito.doCallRealMethod().when( propertyInput ).dispose();

    propertyInput.dispose();

    Method method = PropertyInput.class.getDeclaredMethod( "openNextFile" );
    method.setAccessible( true );
    method.invoke( propertyInput );

    Assert.assertEquals( testValue, propertyInputData.wini.get( header ).get( key ) );
  }

}
