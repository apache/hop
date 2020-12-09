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

package org.apache.hop.pipeline.transforms.jsoninput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class JsonInputMetaLoadSaveTest implements IInitializer<ITransformMeta> {

  static final int FILE_COUNT = new Random().nextInt( 20 ) + 1;
  static final int FIELD_COUNT = new Random().nextInt( 20 ) + 1;

  @Test
  public void testLoadSave() throws HopException {
    List<String> attributes = Arrays.asList( "includeFilename", "filenameField", "includeRowNumber", "addResultFile",
      "ReadUrl", "removeSourceField", "IgnoreEmptyFile", "doNotFailIfNoFile", "ignoreMissingPath", "defaultPathLeafToNull", "rowNumberField",
      "FileName", "FileMask", "ExcludeFileMask", "FileRequired", "IncludeSubFolders", "InputFields", "rowLimit",
      "inFields", "isAFile", "FieldValue", "ShortFileNameField", "PathField", "HiddenField",
      "LastModificationDateField", "UriField", "UriField", "ExtensionField", "SizeField" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    getterMap.put( "includeFilename", "includeFilename" );
    getterMap.put( "includeRowNumber", "includeRowNumber" );
    getterMap.put( "addResultFile", "addResultFile" );

    setterMap.put( "HiddenField", "setIsHiddenField" );

    Map<String, IFieldLoadSaveValidator<?>> attributesMap = new HashMap<>();
    IFieldLoadSaveValidator<?> fileStringArrayValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), FILE_COUNT );

    attributesMap.put( "FileName", fileStringArrayValidator );
    attributesMap.put( "FileMask", fileStringArrayValidator );
    attributesMap.put( "ExcludeFileMask", fileStringArrayValidator );
    attributesMap.put( "FileRequired", fileStringArrayValidator );
    attributesMap.put( "IncludeSubFolders", fileStringArrayValidator );

    Map<String, IFieldLoadSaveValidator<?>> typeMap = new HashMap<>();
    typeMap.put( JsonInputField.class.getCanonicalName(),
      new ArrayLoadSaveValidator<>( new JsonInputFieldValidator() ) );
    typeMap.put( JsonInputField[].class.getCanonicalName(),
      new ArrayLoadSaveValidator<>( new JsonInputFieldValidator() ) );

    LoadSaveTester tester = new LoadSaveTester( JsonInputMeta.class, attributes, new ArrayList<>(), getterMap, setterMap, attributesMap, typeMap, this );

    tester.testSerialization();
  }

  @SuppressWarnings( "deprecation" )
  @Override
  public void modify( ITransformMeta arg0 ) {
    if ( arg0 instanceof JsonInputMeta ) {
      ( (JsonInputMeta) arg0 ).allocate( FILE_COUNT, FIELD_COUNT );
    }
  }

  public static class JsonInputFieldValidator implements IFieldLoadSaveValidator<JsonInputField> {

    @Override
    public JsonInputField getTestObject() {
      JsonInputField retval = new JsonInputField( UUID.randomUUID().toString() );
      return retval;
    }

    @Override
    public boolean validateTestObject( JsonInputField testObject, Object actual ) {
      if ( !( actual instanceof JsonInputField ) ) {
        return false;
      }
      return ( (JsonInputField) actual ).getXml().equals( testObject.getXml() );
    }
  }
}
