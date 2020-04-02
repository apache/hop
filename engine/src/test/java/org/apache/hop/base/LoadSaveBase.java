/* ******************************************************************************
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

package org.apache.hop.base;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.getter.Getter;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.setter.Setter;
import org.apache.hop.pipeline.transforms.loadsave.validator.DefaultFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidatorFactory;
import org.apache.test.util.JavaBeanManipulator;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
public abstract class LoadSaveBase<T> {

  final Class<T> clazz;
  protected final List<String> xmlAttributes;
  protected final JavaBeanManipulator<T> manipulator;
  protected final FieldLoadSaveValidatorFactory fieldLoadSaveValidatorFactory;
  protected final InitializerInterface<T> initializer;
  protected IMetaStore metaStore;

  public LoadSaveBase( Class<T> clazz,
                       List<String> commonAttributes, List<String> xmlAttributes,
                       Map<String, String> getterMap, Map<String, String> setterMap,
                       Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                       Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap,
                       InitializerInterface<T> initializer ) {
    this.clazz = clazz;
    this.xmlAttributes = concat( commonAttributes, xmlAttributes );
    this.manipulator =
      new JavaBeanManipulator<T>( clazz, this.xmlAttributes, getterMap, setterMap );
    this.initializer = initializer;

    Map<Getter<?>, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorMethodMap =
      new HashMap<Getter<?>, FieldLoadSaveValidator<?>>( fieldLoadSaveValidatorAttributeMap.size() );
    for ( Map.Entry<String, FieldLoadSaveValidator<?>> entry : fieldLoadSaveValidatorAttributeMap.entrySet() ) {
      fieldLoadSaveValidatorMethodMap.put( manipulator.getGetter( entry.getKey() ), entry.getValue() );
    }
    this.fieldLoadSaveValidatorFactory =
      new DefaultFieldLoadSaveValidatorFactory( fieldLoadSaveValidatorMethodMap, fieldLoadSaveValidatorTypeMap );
    metaStore = new MemoryMetaStore();
  }

  public LoadSaveBase( Class<T> clazz,
                       List<String> commonAttributes, List<String> xmlAttributes,
                       Map<String, String> getterMap, Map<String, String> setterMap,
                       Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                       Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap ) {
    this( clazz, commonAttributes, xmlAttributes, getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap, null );
  }

  public LoadSaveBase( Class<T> clazz, List<String> commonAttributes ) {
    this( clazz, commonAttributes, new ArrayList<>(),
      new HashMap<>(), new HashMap<>(),
      new HashMap<String, FieldLoadSaveValidator<?>>(), new HashMap<String, FieldLoadSaveValidator<?>>() );
  }

  public T createMeta() {
    try {
      T meta = clazz.newInstance();
      if ( meta instanceof BaseTransformMeta ) {
        TransformMeta mockParentTransformMeta = mock( TransformMeta.class );
        ( (BaseTransformMeta) meta ).setParentTransformMeta( mockParentTransformMeta );
        PipelineMeta mockPipelineMeta = mock( PipelineMeta.class );
        when( mockParentTransformMeta.getParentPipelineMeta() ).thenReturn( mockPipelineMeta );
      }
      return meta;
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to create meta of class " + clazz.getCanonicalName(), e );
    }
  }

  @SuppressWarnings( { "unchecked" } )
  protected Map<String, FieldLoadSaveValidator<?>> createValidatorMapAndInvokeSetters( List<String> attributes,
                                                                                       T metaToSave ) {
    Map<String, FieldLoadSaveValidator<?>> validatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    metaStore = new MemoryMetaStore();
    for ( String attribute : attributes ) {
      Getter<?> getter = manipulator.getGetter( attribute );
      @SuppressWarnings( "rawtypes" )
      Setter setter = manipulator.getSetter( attribute );
      FieldLoadSaveValidator<?> validator = fieldLoadSaveValidatorFactory.createValidator( getter );
      try {
        Object testValue = validator.getTestObject();
        //no-inspection unchecked
        setter.set( metaToSave, testValue );
        if ( testValue instanceof DatabaseMeta ) {
          addDatabase( (DatabaseMeta) testValue );
        } else if ( testValue instanceof DatabaseMeta[] ) {
          addDatabase( (DatabaseMeta[]) testValue );
        }
      } catch ( Exception e ) {
        throw new RuntimeException( "Unable to invoke setter for " + attribute, e );
      }
      validatorMap.put( attribute, validator );
    }
    return validatorMap;
  }

  protected void validateLoadedMeta( List<String> attributes, Map<String, FieldLoadSaveValidator<?>> validatorMap,
                                     T metaSaved, T metaLoaded ) {
    for ( String attribute : attributes ) {
      try {
        Getter<?> getterMethod = manipulator.getGetter( attribute );
        Object originalValue = getterMethod.get( metaSaved );
        Object value = getterMethod.get( metaLoaded );
        FieldLoadSaveValidator<?> validator = validatorMap.get( attribute );
        Method[] validatorMethods = validator.getClass().getMethods();
        Method validatorMethod = null;
        for ( Method method : validatorMethods ) {
          if ( "validateTestObject".equals( method.getName() ) ) {
            Class<?>[] types = method.getParameterTypes();
            if ( types.length == 2 ) {
              if ( types[ 1 ] == Object.class
                && ( originalValue == null || types[ 0 ].isAssignableFrom( originalValue.getClass() ) ) ) {
                validatorMethod = method;
                break;
              }
            }
          }
        }
        if ( validatorMethod == null ) {
          throw new RuntimeException( "Couldn't find proper validateTestObject method on "
            + validator.getClass().getCanonicalName() );
        }
        if ( !( (Boolean) validatorMethod.invoke( validator, originalValue, value ) ) ) {
          throw new HopException( "Attribute " + attribute + " started with value "
            + originalValue + " ended with value " + value );
        }
      } catch ( Exception e ) {
        throw new RuntimeException( "Error validating " + attribute, e );
      }
    }
  }


  private static <E> List<E> concat( List<E> list1, List<E> list2 ) {
    List<E> result = new ArrayList<E>( list1.size() + list2.size() );
    result.addAll( list1 );
    result.addAll( list2 );
    return result;
  }

  protected void addDatabase( DatabaseMeta db ) {
    try {
      DatabaseMeta.createFactory( metaStore ).saveElement( db );
    } catch ( MetaStoreException e ) {
      throw new RuntimeException( "Error adding database to the test metastore", e );
    }
  }

  protected void addDatabase( DatabaseMeta[] db ) {
    if ( db != null ) {
      for ( DatabaseMeta meta : db ) {
        addDatabase( meta );
      }
    }
  }
}
