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

package org.apache.hop.metadata.serializer.json.person.interest;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;

public class InterestObjectFactory implements IHopMetadataObjectFactory {

  @Override public Object createObject( String id, Object parentObject ) throws HopException {
    if ("cooking".equals( id )) {
      return new Cooking();
    }
    if ("running".equals( id )) {
      return new Running();
    }
    if ("music".equals( id )) {
      return new Music();
    }
    throw new HopException("Unable to recognize object ID "+id);
  }

  @Override public String getObjectId( Object object ) throws HopException {
    if (object instanceof Cooking) {
      return "cooking";
    }
    if (object instanceof Running) {
      return "running";
    }
    if (object instanceof Music) {
      return "music";
    }
    throw new HopException("Class "+object.getClass()+" could not be recognized");
  }
}
