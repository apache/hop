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

package org.apache.hop.metadata.api;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

public class HopMetadataDefaultObjectFactory implements IHopMetadataObjectFactory {

  /**
   * By default we use the classname as the id.
   *
   * @param id The ID to use to create the object
   * @return
   * @throws HopException
   */
  @Override public Object createObject( String id, Object parentObject ) throws HopException {
    try {
      Object object = Class.forName( id );

      // By default, inherit variables from a parent object.
      //
      if (parentObject!=null) {
        if (parentObject instanceof IVariables ) {
          if (object instanceof IVariables) {
            ((IVariables)object).initializeFrom( (IVariables) parentObject );
          }
        }
      }

      return object;
    } catch(Exception e) {
      throw new HopException("Unable to create object for id : "+id, e);
    }
  }

  @Override public String getObjectId( Object object ) {
    return object.getClass().getName();
  }
}
