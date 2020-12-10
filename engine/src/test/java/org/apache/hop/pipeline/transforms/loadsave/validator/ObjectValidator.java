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

package org.apache.hop.pipeline.transforms.loadsave.validator;

import org.apache.hop.pipeline.transforms.loadsave.getter.IGetter;
import org.apache.hop.pipeline.transforms.loadsave.setter.ISetter;
import org.apache.test.util.JavaBeanManipulator;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObjectValidator<T> implements IFieldLoadSaveValidator<T> {
  private final IFieldLoadSaveValidatorFactory fieldLoadSaveValidatorFactory;
  private final JavaBeanManipulator<T> manipulator;
  private final Class<T> clazz;
  private final List<String> fieldNames;

  public ObjectValidator( IFieldLoadSaveValidatorFactory fieldLoadSaveValidatorFactory, Class<T> clazz,
                          List<String> fieldNames, Map<String, String> getterMap, Map<String, String> setterMap ) {
    this.fieldLoadSaveValidatorFactory = fieldLoadSaveValidatorFactory;
    manipulator = new JavaBeanManipulator<>( clazz, fieldNames, getterMap, setterMap );
    this.clazz = clazz;
    this.fieldNames = new ArrayList<>( fieldNames );
  }

  public ObjectValidator( IFieldLoadSaveValidatorFactory fieldLoadSaveValidatorFactory, Class<T> clazz,
                          List<String> fieldNames ) {
    this( fieldLoadSaveValidatorFactory, clazz, fieldNames, new HashMap<>(),
      new HashMap<>() );
  }

  @SuppressWarnings( { "rawtypes", "unchecked" } )
  @Override
  public T getTestObject() {
    try {
      T object = clazz.newInstance();
      for ( String attribute : fieldNames ) {
        ISetter setter = manipulator.getSetter( attribute );
        setter.set( object, fieldLoadSaveValidatorFactory.createValidator( manipulator.getGetter( attribute ) )
          .getTestObject() );
      }
      return object;
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to instantiate " + clazz, e );
    }
  }

  @Override
  public boolean validateTestObject( T testObject, Object actual ) {
    if ( actual == null || !( clazz.isAssignableFrom( actual.getClass() ) ) ) {
      return false;
    }
    try {
      for ( String attribute : fieldNames ) {
        IGetter<?> getter = manipulator.getGetter( attribute );
        IFieldLoadSaveValidator<?> validator = fieldLoadSaveValidatorFactory.createValidator( getter );
        Method validatorMethod = null;
        for ( Method method : validator.getClass().getMethods() ) {
          if ( "validateTestObject".equals( method.getName() ) ) {
            Class<?>[] types = method.getParameterTypes();
            if ( types.length == 2 ) {
              validatorMethod = method;
              break;
            }
          }
        }
        if ( validatorMethod == null ) {
          throw new RuntimeException( "Unable to find validator for " + attribute + " " + getter.getGenericType() );
        }
        if ( !(Boolean) validatorMethod.invoke( validator, getter.get( testObject ), getter.get( actual ) ) ) {
          return false;
        }
      }
      return true;
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to instantiate " + clazz, e );
    }
  }
}
