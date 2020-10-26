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

package org.apache.hop.pipeline.transforms.getsubfolders;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GetSubFoldersMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void getFieldsTest() throws HopTransformException {
    GetSubFoldersMeta transformMeta = new GetSubFoldersMeta();
    String transformName = UUID.randomUUID().toString();

    RowMeta rowMeta = new RowMeta();
    transformMeta.getFields( rowMeta, transformName, null, null, new Variables(), null );

    assertFalse( transformMeta.includeRowNumber() );
    assertEquals( 10, rowMeta.size() );
    assertEquals( "folderName", rowMeta.getValueMeta( 0 ).getName() );
    assertEquals( "short_folderName", rowMeta.getValueMeta( 1 ).getName() );
    assertEquals( "path", rowMeta.getValueMeta( 2 ).getName() );
    assertEquals( "ishidden", rowMeta.getValueMeta( 3 ).getName() );
    assertEquals( "isreadable", rowMeta.getValueMeta( 4 ).getName() );
    assertEquals( "iswriteable", rowMeta.getValueMeta( 5 ).getName() );
    assertEquals( "lastmodifiedtime", rowMeta.getValueMeta( 6 ).getName() );
    assertEquals( "uri", rowMeta.getValueMeta( 7 ).getName() );
    assertEquals( "rooturi", rowMeta.getValueMeta( 8 ).getName() );
    assertEquals( "childrens", rowMeta.getValueMeta( 9 ).getName() );

    transformMeta.setIncludeRowNumber( true );
    rowMeta = new RowMeta();
    transformMeta.getFields( rowMeta, transformName, null, null, new Variables(), null );
    assertTrue( transformMeta.includeRowNumber() );
    assertEquals( 11, rowMeta.size() );
    assertEquals( "folderName", rowMeta.getValueMeta( 0 ).getName() );
    assertEquals( "short_folderName", rowMeta.getValueMeta( 1 ).getName() );
    assertEquals( "path", rowMeta.getValueMeta( 2 ).getName() );
    assertEquals( "ishidden", rowMeta.getValueMeta( 3 ).getName() );
    assertEquals( "isreadable", rowMeta.getValueMeta( 4 ).getName() );
    assertEquals( "iswriteable", rowMeta.getValueMeta( 5 ).getName() );
    assertEquals( "lastmodifiedtime", rowMeta.getValueMeta( 6 ).getName() );
    assertEquals( "uri", rowMeta.getValueMeta( 7 ).getName() );
    assertEquals( "rooturi", rowMeta.getValueMeta( 8 ).getName() );
    assertEquals( "childrens", rowMeta.getValueMeta( 9 ).getName() );
    assertEquals( null, rowMeta.getValueMeta( 10 ).getName() );

    transformMeta.setRowNumberField( "MyRowNumber" );
    rowMeta = new RowMeta();
    transformMeta.getFields( rowMeta, transformName, null, null, new Variables(), null );
    assertEquals( "MyRowNumber", transformMeta.getRowNumberField() );
    assertEquals( 11, rowMeta.size() );
    assertEquals( "MyRowNumber", rowMeta.getValueMeta( 10 ).getName() );
  }

  @Test
  public void loadSaveTest() throws HopException {
    List<String> attributes =
      Arrays.asList( "rownum", "foldername_dynamic", "rownum_field",
        "foldername_field", "limit", "name", "file_required" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "rownum", "includeRowNumber" );
    getterMap.put( "foldername_dynamic", "isFoldernameDynamic" );
    getterMap.put( "foldername_field", "getDynamicFoldernameField" );
    getterMap.put( "rownum_field", "getRowNumberField" );
    getterMap.put( "limit", "getRowLimit" );
    getterMap.put( "name", "getFolderName" );
    getterMap.put( "file_required", "getFolderRequired" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "rownum", "setIncludeRowNumber" );
    setterMap.put( "foldername_dynamic", "setFolderField" );
    setterMap.put( "foldername_field", "setDynamicFoldernameField" );
    setterMap.put( "rownum_field", "setRowNumberField" );
    setterMap.put( "limit", "setRowLimit" );
    setterMap.put( "name", "setFolderName" );
    setterMap.put( "file_required", "setFolderRequired" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();
    fieldLoadSaveValidatorAttributeMap.put( "file_required",
      new ArrayLoadSaveValidator<String>( new FileRequiredFieldLoadSaveValidator(), 50 ) );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    fieldLoadSaveValidatorTypeMap.put( String[].class.getCanonicalName(),
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 50 ) );

    LoadSaveTester tester = new LoadSaveTester( GetSubFoldersMeta.class, attributes, getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );

    tester.testSerialization();
  }

  public class FileRequiredFieldLoadSaveValidator implements FieldLoadSaveValidator<String> {

    @Override
    public String getTestObject() {
      return GetSubFoldersMeta.RequiredFoldersCode[ new Random().nextInt( GetSubFoldersMeta.RequiredFoldersCode.length ) ];
    }

    @Override
    public boolean validateTestObject( String testObject, Object actual ) {
      return testObject.equals( actual );
    }
  }
}
