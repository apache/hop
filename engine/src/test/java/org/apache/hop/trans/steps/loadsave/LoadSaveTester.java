/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.trans.steps.loadsave;

import org.apache.hop.base.LoadSaveBase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.steps.loadsave.getter.Getter;
import org.apache.hop.trans.steps.loadsave.initializer.InitializerInterface;
import org.apache.hop.trans.steps.loadsave.setter.Setter;
import org.apache.hop.trans.steps.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidatorFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotSame;

public class LoadSaveTester<T extends StepMetaInterface> extends LoadSaveBase<T> {

  public LoadSaveTester( Class<T> clazz,
                         List<String> commonAttributes, List<String> xmlAttributes,
                         Map<String, String> getterMap, Map<String, String> setterMap,
                         Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                         Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap,
                         InitializerInterface<T> metaInitializerIFace ) {
    super( clazz, commonAttributes, xmlAttributes, getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap, metaInitializerIFace );
  }

  public LoadSaveTester( Class<T> clazz,
                         List<String> commonAttributes, List<String> xmlAttributes,
                         Map<String, String> getterMap, Map<String, String> setterMap,
                         Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                         Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap ) {
    this( clazz, commonAttributes, xmlAttributes, getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap, null );
  }

  public LoadSaveTester( Class<T> clazz, List<String> commonAttributes,
                         Map<String, String> getterMap, Map<String, String> setterMap,
                         Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                         Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap ) {
    this( clazz, commonAttributes, new ArrayList<>(), getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );
  }

  public LoadSaveTester( Class<T> clazz, List<String> commonAttributes,
                         Map<String, String> getterMap, Map<String, String> setterMap,
                         Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                         Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap,
                         InitializerInterface<T> metaInitializerIFace ) {
    this( clazz, commonAttributes, new ArrayList<>(), getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap, metaInitializerIFace );
  }

  public LoadSaveTester( Class<T> clazz, List<String> commonAttributes,
                         List<String> xmlAttributes,
                         Map<String, String> getterMap, Map<String, String> setterMap ) {
    this( clazz, commonAttributes, xmlAttributes, getterMap, setterMap,
      new HashMap<String, FieldLoadSaveValidator<?>>(), new HashMap<String, FieldLoadSaveValidator<?>>() );
  }

  public LoadSaveTester( Class<T> clazz, List<String> commonAttributes,
                         Map<String, String> getterMap, Map<String, String> setterMap ) {
    this( clazz, commonAttributes, new ArrayList<>(), getterMap, setterMap,
      new HashMap<String, FieldLoadSaveValidator<?>>(), new HashMap<String, FieldLoadSaveValidator<?>>() );
  }

  public LoadSaveTester( Class<T> clazz, List<String> commonAttributes ) {
    this( clazz, commonAttributes, new HashMap<>(), new HashMap<>() );
  }

  public FieldLoadSaveValidatorFactory getFieldLoadSaveValidatorFactory() {
    return fieldLoadSaveValidatorFactory;
  }

  @Override
  @SuppressWarnings( "unchecked" )
  protected Map<String, FieldLoadSaveValidator<?>> createValidatorMapAndInvokeSetters( List<String> attributes,
                                                                                       T metaToSave ) {
    Map<String, FieldLoadSaveValidator<?>> validatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    for ( String attribute : attributes ) {
      Getter<?> getter = manipulator.getGetter( attribute );
      @SuppressWarnings( "rawtypes" )
      Setter setter = manipulator.getSetter( attribute );
      FieldLoadSaveValidator<?> validator = fieldLoadSaveValidatorFactory.createValidator( getter );
      try {
        Object testValue = validator.getTestObject();
        setter.set( metaToSave, testValue );
        if ( validator instanceof DatabaseMetaLoadSaveValidator ) {
          addDatabase( (DatabaseMeta) testValue );
        }
      } catch ( Exception e ) {
        throw new RuntimeException( "Unable to invoke setter for " + attribute, e );
      }
      validatorMap.put( attribute, validator );
    }
    return validatorMap;
  }

  public void testSerialization() throws HopException {
    testXmlRoundTrip();
    testClone();
  }

  @SuppressWarnings( { "deprecation", "unchecked" } )
  protected void testClone() throws HopException {
    T metaToSave = createMeta();
    if ( initializer != null ) {
      initializer.modify( metaToSave );
    }
    Map<String, FieldLoadSaveValidator<?>> validatorMap =
      createValidatorMapAndInvokeSetters( xmlAttributes, metaToSave );

    T metaLoaded = (T) metaToSave.clone();
    assertNotSame( metaToSave, metaLoaded );
    validateLoadedMeta( xmlAttributes, validatorMap, metaToSave, metaLoaded );
  }

  /**
   * @throws HopException
   * @deprecated the {@link #testSerialization()} method should be used instead,
   * as additional tests may be added in the future to cover other
   * topics related to step serialization
   */
  @Deprecated
  // TODO Change method visibility to protected
  public void testXmlRoundTrip() throws HopException {
    T metaToSave = createMeta();
    if ( initializer != null ) {
      initializer.modify( metaToSave );
    }
    Map<String, FieldLoadSaveValidator<?>> validatorMap =
      createValidatorMapAndInvokeSetters( xmlAttributes, metaToSave );
    T metaLoaded = createMeta();
    String xml = "<step>" + metaToSave.getXML() + "</step>";
    InputStream is = new ByteArrayInputStream( xml.getBytes() );
    metaLoaded.loadXML( XMLHandler.getSubNode( XMLHandler.loadXMLFile( is, null, false, false ), "step" ),
      metaStore );
    validateLoadedMeta( xmlAttributes, validatorMap, metaToSave, metaLoaded );

    // TODO Remove after method visibility changed, it should be called in testSerialization
    testClone();
  }

}
