/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.selectvalues;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.selectvalues.SelectValuesMeta.SelectField;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SelectValuesMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static final String FIRST_FIELD = "FIRST_FIELD";

  private static final String SECOND_FIELD = "SECOND_FIELD";

  private SelectValuesMeta selectValuesMeta;

  @Before
  public void before() {
    selectValuesMeta = new SelectValuesMeta();
  }

  @Ignore
  @Test
  public void loadSaveTest() throws HopException {
    List<String> attributes = Arrays.asList( "selectFields", "deleteName" );

    SelectField selectField = new SelectField();
    selectField.setName( "TEST_NAME" );
    selectField.setRename( "TEST_RENAME" );
    selectField.setLength( 2 );
    selectField.setPrecision( 2 );

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap =
      new HashMap<String, IFieldLoadSaveValidator<?>>();
    fieldLoadSaveValidatorTypeMap.put( SelectField[].class.getCanonicalName(), new ArrayLoadSaveValidator<SelectField>(
      new SelectFieldLoadSaveValidator( selectField ), 2 ) );

    LoadSaveTester tester =
      new LoadSaveTester( SelectValuesMeta.class, attributes, new HashMap<>(),
        new HashMap<>(), new HashMap<String, IFieldLoadSaveValidator<?>>(),
        fieldLoadSaveValidatorTypeMap );

    tester.testSerialization();
  }

  @Test
  public void setSelectName() {
    selectValuesMeta.setSelectName( new String[] { FIRST_FIELD, SECOND_FIELD } );
    assertArrayEquals( new String[] { FIRST_FIELD, SECOND_FIELD }, selectValuesMeta.getSelectName() );
  }

  @Test
  public void setSelectName_getOtherFields() {
    selectValuesMeta.setSelectName( new String[] { FIRST_FIELD, SECOND_FIELD } );
    assertArrayEquals( new String[] { null, null }, selectValuesMeta.getSelectRename() );
    assertArrayEquals( new int[] { SelectValuesMeta.UNDEFINED, SelectValuesMeta.UNDEFINED }, selectValuesMeta
      .getSelectLength() );
    assertArrayEquals( new int[] { SelectValuesMeta.UNDEFINED, SelectValuesMeta.UNDEFINED }, selectValuesMeta
      .getSelectPrecision() );
  }

  @Test
  public void setSelectName_smallerThanPrevious() {
    selectValuesMeta.setSelectName( new String[] { FIRST_FIELD, SECOND_FIELD } );
    selectValuesMeta.setSelectName( new String[] { FIRST_FIELD } );
    assertArrayEquals( new String[] { FIRST_FIELD }, selectValuesMeta.getSelectName() );
  }

  @Test
  public void getSelectName() {
    assertArrayEquals( new String[ 0 ], selectValuesMeta.getSelectName() );
  }

  @Test
  public void setSelectRename() {
    selectValuesMeta.setSelectRename( new String[] { FIRST_FIELD, SECOND_FIELD } );
    assertArrayEquals( new String[] { FIRST_FIELD, SECOND_FIELD }, selectValuesMeta.getSelectRename() );
  }

  @Test
  public void setSelectRename_getOtherFields() {
    selectValuesMeta.setSelectRename( new String[] { FIRST_FIELD, SECOND_FIELD } );
    assertArrayEquals( new String[] { null, null }, selectValuesMeta.getSelectName() );
    assertArrayEquals( new int[] { SelectValuesMeta.UNDEFINED, SelectValuesMeta.UNDEFINED }, selectValuesMeta
      .getSelectLength() );
    assertArrayEquals( new int[] { SelectValuesMeta.UNDEFINED, SelectValuesMeta.UNDEFINED }, selectValuesMeta
      .getSelectPrecision() );
  }

  @Test
  public void setSelectRename_smallerThanPrevious() {
    selectValuesMeta.setSelectRename( new String[] { FIRST_FIELD, SECOND_FIELD } );
    selectValuesMeta.setSelectRename( new String[] { FIRST_FIELD } );
    assertArrayEquals( new String[] { FIRST_FIELD, null }, selectValuesMeta.getSelectRename() );
  }

  @Test
  public void getSelectRename() {
    assertArrayEquals( new String[ 0 ], selectValuesMeta.getSelectRename() );
  }

  @Test
  public void setSelectLength() {
    selectValuesMeta.setSelectLength( new int[] { 1, 2 } );
    assertArrayEquals( new int[] { 1, 2 }, selectValuesMeta.getSelectLength() );
  }

  @Test
  public void setSelectLength_getOtherFields() {
    selectValuesMeta.setSelectLength( new int[] { 1, 2 } );
    assertArrayEquals( new String[] { null, null }, selectValuesMeta.getSelectName() );
    assertArrayEquals( new String[] { null, null }, selectValuesMeta.getSelectRename() );
    assertArrayEquals( new int[] { SelectValuesMeta.UNDEFINED, SelectValuesMeta.UNDEFINED }, selectValuesMeta
      .getSelectPrecision() );
  }

  @Test
  public void setSelectLength_smallerThanPrevious() {
    selectValuesMeta.setSelectLength( new int[] { 1, 2 } );
    selectValuesMeta.setSelectLength( new int[] { 1 } );
    assertArrayEquals( new int[] { 1, SelectValuesMeta.UNDEFINED }, selectValuesMeta.getSelectLength() );
  }

  @Test
  public void getSelectLength() {
    assertArrayEquals( new int[ 0 ], selectValuesMeta.getSelectLength() );
  }

  @Test
  public void setSelectPrecision() {
    selectValuesMeta.setSelectPrecision( new int[] { 1, 2 } );
    assertArrayEquals( new int[] { 1, 2 }, selectValuesMeta.getSelectPrecision() );
  }

  @Test
  public void setSelectPrecision_getOtherFields() {
    selectValuesMeta.setSelectPrecision( new int[] { 1, 2 } );
    assertArrayEquals( new String[] { null, null }, selectValuesMeta.getSelectName() );
    assertArrayEquals( new String[] { null, null }, selectValuesMeta.getSelectRename() );
    assertArrayEquals( new int[] { SelectValuesMeta.UNDEFINED, SelectValuesMeta.UNDEFINED }, selectValuesMeta
      .getSelectLength() );
  }

  @Test
  public void setSelectPrecision_smallerThanPrevious() {
    selectValuesMeta.setSelectPrecision( new int[] { 1, 2 } );
    selectValuesMeta.setSelectPrecision( new int[] { 1 } );
    assertArrayEquals( new int[] { 1, SelectValuesMeta.UNDEFINED }, selectValuesMeta.getSelectPrecision() );
  }

  @Test
  public void setSelectFieldsNull() {
    selectValuesMeta.setSelectFields( null );
    assertNotNull( selectValuesMeta.getSelectFields() );
    assertEquals( 0, selectValuesMeta.getSelectFields().length );
  }

  @Test
  public void setDeleteNameNull() {
    selectValuesMeta.setDeleteName( null );
    assertNotNull( selectValuesMeta.getDeleteName() );
    assertEquals( 0, selectValuesMeta.getDeleteName().length );
  }

  @Test
  public void setMetaNull() {
    selectValuesMeta.setMeta( null );
    assertNotNull( selectValuesMeta.getMeta() );
    assertEquals( 0, selectValuesMeta.getMeta().length );
  }

  @Test
  public void getSelectPrecision() {
    assertArrayEquals( new int[ 0 ], selectValuesMeta.getSelectPrecision() );
  }

  public static class SelectFieldLoadSaveValidator implements IFieldLoadSaveValidator<SelectField> {

    private final SelectField defaultValue;

    public SelectFieldLoadSaveValidator( SelectField defaultValue ) {
      this.defaultValue = defaultValue;
    }

    @Override
    public SelectField getTestObject() {
      return defaultValue;
    }

    @Override
    public boolean validateTestObject( SelectField testObject, Object actual ) {
      return EqualsBuilder.reflectionEquals( testObject, actual );
    }
  }
}
