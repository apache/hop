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

import java.util.Random;

public class PrimitiveIntArrayLoadSaveValidator implements IFieldLoadSaveValidator<int[]> {
  private final IFieldLoadSaveValidator<Integer> validator;
  private final Integer elements;

  public PrimitiveIntArrayLoadSaveValidator( IFieldLoadSaveValidator<Integer> fieldValidator ) {
    this( fieldValidator, null );
  }

  public PrimitiveIntArrayLoadSaveValidator( IFieldLoadSaveValidator<Integer> fieldValidator, Integer elements ) {
    validator = fieldValidator;
    this.elements = elements;
  }

  @Override
  public int[] getTestObject() {
    int max = elements == null ? new Random().nextInt( 100 ) + 50 : elements;
    int[] result = new int[ max ];
    for ( int i = 0; i < max; i++ ) {
      result[ i ] = validator.getTestObject();
    }
    return result;
  }

  @Override
  public boolean validateTestObject( int[] original, Object actual ) {
    if ( original.getClass().isAssignableFrom( actual.getClass() ) ) {
      int[] otherList = (int[]) actual;
      if ( original.length != otherList.length ) {
        return false;
      }
      for ( int i = 0; i < original.length; i++ ) {
        if ( !this.validator.validateTestObject( original[ i ], otherList[ i ] ) ) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
