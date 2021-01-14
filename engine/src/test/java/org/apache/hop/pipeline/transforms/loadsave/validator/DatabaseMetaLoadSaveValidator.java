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

import org.apache.hop.core.database.DatabaseMeta;

import java.util.Random;
import java.util.UUID;

public class DatabaseMetaLoadSaveValidator implements IFieldLoadSaveValidator<DatabaseMeta> {

  private static final Random rand = new Random();

  @Override
  public DatabaseMeta getTestObject() {
    DatabaseMeta db = new DatabaseMeta();
    db.setName( UUID.randomUUID().toString() );
    db.setHostname( UUID.randomUUID().toString() );
    db.setUsername( UUID.randomUUID().toString() );
    db.setPassword( UUID.randomUUID().toString() );
    return db;
  }

  @Override
  public boolean validateTestObject( DatabaseMeta testObject, Object actual ) {
    if ( actual instanceof DatabaseMeta ) {
      return testObject.equals( actual );
    }
    return false;
  }

}
