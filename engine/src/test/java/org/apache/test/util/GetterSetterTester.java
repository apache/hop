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

package org.apache.test.util;

import org.apache.hop.pipeline.transforms.loadsave.setter.ISetter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class GetterSetterTester<T> {
  private final Map<String, IObjectTester<?>> objectTesterMap;
  private final Class<? extends T> clazz;
  private final Map<String, String> getterMap;
  private final Map<String, String> setterMap;

  public GetterSetterTester( Class<? extends T> clazz ) {
    this( clazz, new HashMap<>(), new HashMap<>() );
  }

  public GetterSetterTester( Class<? extends T> clazz, Map<String, String> getterMap, Map<String, String> setterMap ) {
    this.clazz = clazz;
    this.getterMap = getterMap;
    this.setterMap = setterMap;
    objectTesterMap = new HashMap<>();
  }

  public void test( Object objectUnderTest ) {
    JavaBeanManipulator<T> manipulator =
      new JavaBeanManipulator<>( clazz, new ArrayList<>( objectTesterMap.keySet() ), getterMap, setterMap );
    for ( Entry<String, IObjectTester<?>> entry : objectTesterMap.entrySet() ) {
      String attribute = entry.getKey();
      @SuppressWarnings( "unchecked" )
      IObjectTester<Object> tester = (IObjectTester<Object>) entry.getValue();
      for ( Object testObject : tester.getTestObjects() ) {
        @SuppressWarnings( "unchecked" )
        ISetter<Object> setter = (ISetter<Object>) manipulator.getSetter( attribute );
        setter.set( objectUnderTest, testObject );
        tester.validate( testObject, manipulator.getGetter( attribute ).get( objectUnderTest ) );
      }
    }
  }

  public void addObjectTester( String attribute, IObjectTester<?> objectTester ) {
    objectTesterMap.put( attribute, objectTester );
  }
}
