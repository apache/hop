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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class DimensionLookupMetaInjectionTest extends BaseMetadataInjectionTest<DimensionLookupMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    super.setup( new DimensionLookupMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "TARGET_SCHEMA", () -> meta.getSchemaName() );
    check( "TARGET_TABLE", () -> meta.getTableName() );
    check( "UPDATE_DIMENSION", () -> meta.isUpdate() );
    check( "KEY_STREAM_FIELDNAME", () -> meta.getKeyStream()[ 0 ] );
    check( "KEY_DATABASE_FIELDNAME", () -> meta.getKeyLookup()[ 0 ] );
    check( "STREAM_DATE_FIELD", () -> meta.getDateField() );
    check( "DATE_RANGE_START_FIELD", () -> meta.getDateFrom() );
    check( "DATE_RANGE_END_FIELD", () -> meta.getDateTo() );
    check( "STREAM_FIELDNAME", () -> meta.getFieldStream()[ 0 ] );
    check( "DATABASE_FIELDNAME", () -> meta.getFieldLookup()[ 0 ] );
    check( "TECHNICAL_KEY_FIELD", () -> meta.getKeyField() );
    check( "TECHNICAL_KEY_NEW_NAME", () -> meta.getKeyRename() );
    check( "VERSION_FIELD", () -> meta.getVersionField() );
    check( "TECHNICAL_KEY_SEQUENCE", () -> meta.getSequenceName() );
    check( "COMMIT_SIZE", () -> meta.getCommitSize() );
    check( "MIN_YEAR", () -> meta.getMinYear() );
    check( "MAX_YEAR", () -> meta.getMaxYear() );
    check( "TECHNICAL_KEY_CREATION", () -> meta.getTechKeyCreation() );
    check( "CACHE_SIZE", () -> meta.getCacheSize() );
    check( "USE_ALTERNATIVE_START_DATE", () -> meta.isUsingStartDateAlternative() );
    check( "ALTERNATIVE_START_COLUMN", () -> meta.getStartDateFieldName() );
    check( "PRELOAD_CACHE", () -> meta.isPreloadingCache() );
    check( "CONNECTION_NAME", () -> "My Connection", "My Connection" );

    IValueMeta mftt = new ValueMetaString( "f" );
    injector.setProperty( meta, "ALTERNATIVE_START_OPTION", setValue( mftt, DimensionLookupMeta
      .getStartDateAlternativeCode( 0 ) ), "f" );
    Assert.assertEquals( 0, meta.getStartDateAlternative() );

    String[] valueMetaNames = ValueMetaFactory.getValueMetaNames();
    checkStringToInt( "TYPE_OF_RETURN_FIELD", () -> meta.getReturnType()[ 0 ], valueMetaNames, getTypeCodes( valueMetaNames ) );

    skipPropertyTest( "ALTERNATIVE_START_OPTION" );

    skipPropertyTest( "UPDATE_TYPE" );
  }

}
