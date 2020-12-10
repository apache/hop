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

package org.apache.hop.core.parameters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is an implementation of INamedParameters.
 *
 * @author Sven Boden
 */
public class NamedParametersDefnitions implements INamedParameterDefinitions {
  /**
   * Map to store named parameters in.
   */
  protected Map<String, NamedParameterDefinition> params = new HashMap<>();

  /**
   * Default constructor.
   */
  public NamedParametersDefnitions() {
  }

  @Override
  public void addParameterDefinition( String key, String defValue, String description ) throws DuplicateParamException {

    if ( params.get( key ) == null ) {
      NamedParameterDefinition oneParam = new NamedParameterDefinition();

      oneParam.key = key;
      oneParam.defaultValue = defValue;
      oneParam.description = description;

      params.put( key, oneParam );
    } else {
      throw new DuplicateParamException( "Duplicate parameter '" + key + "' detected." );
    }
  }

  @Override
  public String getParameterDescription( String key ) throws UnknownParamException {
    String description = null;

    NamedParameterDefinition theParam = params.get( key );
    if ( theParam != null ) {
      description = theParam.description;
    }

    return description;
  }

  @Override
  public String getParameterDefault( String key ) throws UnknownParamException {
    String value = null;

    NamedParameterDefinition theParam = params.get( key );
    if ( theParam != null ) {
      value = theParam.defaultValue;
    }

    return value;
  }

  @Override
  public String[] listParameters() {
    Set<String> keySet = params.keySet();

    String[] paramArray = keySet.toArray( new String[ 0 ] );
    Arrays.sort( paramArray );

    return paramArray;
  }

  @Override public void removeAllParameters() {
    params.clear();
  }
}
