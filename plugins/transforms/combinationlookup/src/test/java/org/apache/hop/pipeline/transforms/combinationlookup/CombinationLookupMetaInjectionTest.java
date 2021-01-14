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

package org.apache.hop.pipeline.transforms.combinationlookup;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CombinationLookupMetaInjectionTest extends BaseMetadataInjectionTest<CombinationLookupMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new CombinationLookupMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SCHEMA_NAME", () -> meta.getSchemaName() );
    check( "TABLE_NAME", () -> meta.getTableName() );
    check( "REPLACE_FIELDS", () -> meta.replaceFields() );
    check( "KEY_FIELDS", () -> meta.getKeyField()[ 0 ] );
    check( "KEY_LOOKUP", () -> meta.getKeyLookup()[ 0 ] );
    check( "USE_HASH", () -> meta.useHash() );
    check( "HASH_FIELD", () -> meta.getHashField() );
    check( "TECHNICAL_KEY_FIELD", () -> meta.getTechnicalKeyField() );
    check( "SEQUENCE_FROM", () -> meta.getSequenceFrom() );
    check( "COMMIT_SIZE", () -> meta.getCommitSize() );
    check( "PRELOAD_CACHE", () -> meta.getPreloadCache() );
    check( "CACHE_SIZE", () -> meta.getCacheSize() );
    check( "AUTO_INC", () -> meta.isUseAutoinc() );
    check( "TECHNICAL_KEY_CREATION", () -> meta.getTechKeyCreation() );
    check( "LAST_UPDATE_FIELD", () -> meta.getLastUpdateField() );
    skipPropertyTest( "CONNECTIONNAME" );
  }
}
