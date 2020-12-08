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

package org.apache.hop.base;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.getter.IGetter;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.setter.ISetter;
import org.apache.hop.pipeline.transforms.loadsave.validator.DefaultFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
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
  protected final IFieldLoadSaveValidatorFactory fieldLoadSaveValidatorFactory;
  protected final IInitializer<T> initializer;
  protected IHopMetadataProvider metadataProvider;

  public LoadSaveBase( Class<T> clazz,
                       List<String> commonAttributes, List<String> xmlAttributes,
                       Map<String, String> getterMap, Map<String, String> setterMap,
                       Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                       Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap,
                       IInitializer<T> initializer ) {
    this.clazz = clazz;
    this.xmlAttributes = concat( commonAttributes, xmlAttributes );
    this.manipulator =
      new JavaBeanManipulator<>( clazz, this.xmlAttributes, getterMap, setterMap );
    this.initializer = initializer;

    Map<IGetter<?>, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorMethodMap =
      new HashMap<>( fieldLoadSaveValidatorAttributeMap.size() );
    for ( Map.Entry<String, IFieldLoadSaveValidator<?>> entry : fieldLoadSaveValidatorAttributeMap.entrySet() ) {
      fieldLoadSaveValidatorMethodMap.put( manipulator.getGetter( entry.getKey() ), entry.getValue() );
    }
    this.fieldLoadSaveValidatorFactory =
      new DefaultFieldLoadSaveValidatorFactory( fieldLoadSaveValidatorMethodMap, fieldLoadSaveValidatorTypeMap );
    metadataProvider = new MemoryMetadataProvider();
  }

  public LoadSaveBase( Class<T> clazz,
                       List<String> commonAttributes, List<String> xmlAttributes,
                       Map<String, String> getterMap, Map<String, String> setterMap,
                       Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                       Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap ) {
    this( clazz, commonAttributes, xmlAttributes, getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap, null );
  }

  public LoadSaveBase( Class<T> clazz, List<String> commonAttributes ) {
    this( clazz, commonAttributes, new ArrayList<>(),
      new HashMap<>(), new HashMap<>(),
      new HashMap<>(), new HashMap<>() );
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
  protected Map<String, IFieldLoadSaveValidator<?>> createValidatorMapAndInvokeSetters( List<String> attributes,
                                                                                        T metaToSave ) {
    Map<String, IFieldLoadSaveValidator<?>> validatorMap = new HashMap<>();
    metadataProvider = new MemoryMetadataProvider();
    for ( String attribute : attributes ) {
      IGetter<?> getter = manipulator.getGetter( attribute );
      @SuppressWarnings( "rawtypes" )
      ISetter setter = manipulator.getSetter( attribute );
      IFieldLoadSaveValidator<?> validator = fieldLoadSaveValidatorFactory.createValidator( getter );
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

  protected void validateLoadedMeta( List<String> attributes, Map<String, IFieldLoadSaveValidator<?>> validatorMap,
                                     T metaSaved, T metaLoaded ) {
    for ( String attribute : attributes ) {
      try {
        IGetter<?> getterMethod = manipulator.getGetter( attribute );
        Object originalValue = getterMethod.get( metaSaved );
        Object value = getterMethod.get( metaLoaded );
        IFieldLoadSaveValidator<?> validator = validatorMap.get( attribute );
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
    List<E> result = new ArrayList<>( list1.size() + list2.size() );
    result.addAll( list1 );
    result.addAll( list2 );
    return result;
  }

  protected void addDatabase( DatabaseMeta db ) {
    try {
      metadataProvider.getSerializer( DatabaseMeta.class ).save( db );
    } catch ( HopException e ) {
      throw new RuntimeException( "Error adding database to the test metadata", e );
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
