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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class MapLoadSaveValidator<KeyObjectType, ValueObjectType> implements
  IFieldLoadSaveValidator<Map<KeyObjectType, ValueObjectType>> {
  private final IFieldLoadSaveValidator<KeyObjectType> keyValidator;
  private final IFieldLoadSaveValidator<ValueObjectType> valueValidator;
  private final Integer elements;

  public MapLoadSaveValidator( IFieldLoadSaveValidator<KeyObjectType> keyFieldValidator,
                               IFieldLoadSaveValidator<ValueObjectType> valueFieldValidator ) {
    keyValidator = keyFieldValidator;
    valueValidator = valueFieldValidator;
    elements = null;
  }

  public MapLoadSaveValidator( IFieldLoadSaveValidator<KeyObjectType> keyFieldValidator,
                               IFieldLoadSaveValidator<ValueObjectType> valueFieldValidator, Integer elements ) {
    keyValidator = keyFieldValidator;
    valueValidator = valueFieldValidator;
    this.elements = elements;
  }

  @Override
  public Map<KeyObjectType, ValueObjectType> getTestObject() {
    int max = elements == null ? new Random().nextInt( 100 ) + 50 : elements;
    Map<KeyObjectType, ValueObjectType> result = new LinkedHashMap<>();
    for ( int i = 0; i < max; i++ ) {
      result.put( keyValidator.getTestObject(), valueValidator.getTestObject() );
    }
    return result;
  }

  @Override
  public boolean validateTestObject( Map<KeyObjectType, ValueObjectType> original, Object actual ) {
    if ( actual instanceof Map ) {
      @SuppressWarnings( "unchecked" )
      Map<KeyObjectType, ValueObjectType> actualMap = (Map<KeyObjectType, ValueObjectType>) actual;
      if ( original.size() != actualMap.size() ) {
        return false;
      }
      for ( KeyObjectType originalKey : original.keySet() ) {
        if ( !actualMap.containsKey( originalKey ) ) {
          return false;
        }
        if ( !this.valueValidator.validateTestObject( original.get( originalKey ), actualMap.get( originalKey ) ) ) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
