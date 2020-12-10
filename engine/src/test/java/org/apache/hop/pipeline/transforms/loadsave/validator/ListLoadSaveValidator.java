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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ListLoadSaveValidator<ObjectType> implements
  IFieldLoadSaveValidator<List<ObjectType>> {
  private final IFieldLoadSaveValidator<ObjectType> validator;
  private final Integer elements;

  public ListLoadSaveValidator( IFieldLoadSaveValidator<ObjectType> fieldValidator ) {
    validator = fieldValidator;
    elements = null;
  }

  public ListLoadSaveValidator( IFieldLoadSaveValidator<ObjectType> fieldValidator, Integer elements ) {
    validator = fieldValidator;
    this.elements = elements;
  }

  @Override
  public List<ObjectType> getTestObject() {
    int max = elements == null ? new Random().nextInt( 100 ) + 50 : elements;
    List<ObjectType> result = new ArrayList<>( max );
    for ( int i = 0; i < max; i++ ) {
      result.add( validator.getTestObject() );
    }
    return result;
  }

  @Override
  public boolean validateTestObject( List<ObjectType> original, Object actual ) {
    if ( actual instanceof List ) {
      List<?> otherList = (List<?>) actual;
      if ( original.size() != otherList.size() ) {
        return false;
      }
      for ( int i = 0; i < original.size(); i++ ) {
        if ( !this.validator.validateTestObject( original.get( i ), otherList.get( i ) ) ) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

}
