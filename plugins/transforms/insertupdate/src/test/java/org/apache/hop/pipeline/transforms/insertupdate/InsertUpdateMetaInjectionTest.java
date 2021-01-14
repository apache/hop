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

package org.apache.hop.pipeline.transforms.insertupdate;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class InsertUpdateMetaInjectionTest extends BaseMetadataInjectionTest<InsertUpdateMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new InsertUpdateMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SCHEMA_NAME", () -> meta.getSchemaName() );
    check( "TABLE_NAME", () -> meta.getTableName() );
    check( "COMMIT_SIZE", () -> meta.getCommitSizeVar() );
    check( "DO_NOT", () -> meta.isUpdateBypassed() );
    check( "KEY_STREAM", () -> meta.getKeyStream()[ 0 ] );
    check( "KEY_LOOKUP", () -> meta.getKeyLookup()[ 0 ] );
    check( "KEY_CONDITION", () -> meta.getKeyCondition()[ 0 ] );
    check( "KEY_STREAM2", () -> meta.getKeyStream2()[ 0 ] );
    check( "UPDATE_LOOKUP", () -> meta.getUpdateLookup()[ 0 ] );
    check( "UPDATE_STREAM", () -> meta.getUpdateStream()[ 0 ] );
    check( "UPDATE_FLAG", () -> meta.getUpdate()[ 0 ] );
    skipPropertyTest( "CONNECTIONNAME" );
  }
}
