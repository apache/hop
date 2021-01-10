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

package org.apache.hop.pipeline.transforms.selectvalues;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SelectValuesMetaInjectionTest extends BaseMetadataInjectionTest<SelectValuesMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new SelectValuesMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SELECT_UNSPECIFIED", () -> meta.isSelectingAndSortingUnspecifiedFields() );
    check( "FIELD_NAME", () -> meta.getSelectFields()[ 0 ].getName() );
    check( "FIELD_RENAME", () -> meta.getSelectFields()[ 0 ].getRename() );
    check( "FIELD_LENGTH", () -> meta.getSelectFields()[ 0 ].getLength() );
    check( "FIELD_PRECISION", () -> meta.getSelectFields()[ 0 ].getPrecision() );
    check( "REMOVE_NAME", () -> meta.getDeleteName()[ 0 ] );
    check( "META_NAME", () -> meta.getMeta()[ 0 ].getName() );
    check( "META_RENAME", () -> meta.getMeta()[ 0 ].getRename() );
    check( "META_LENGTH", () -> meta.getMeta()[ 0 ].getLength() );
    check( "META_PRECISION", () -> meta.getMeta()[ 0 ].getPrecision() );
    check( "META_CONVERSION_MASK", () -> meta.getMeta()[ 0 ].getConversionMask() );
    check( "META_DATE_FORMAT_LENIENT", () -> meta.getMeta()[ 0 ].isDateFormatLenient() );
    check( "META_DATE_FORMAT_LOCALE", () -> meta.getMeta()[ 0 ].getDateFormatLocale() );
    check( "META_DATE_FORMAT_TIMEZONE", () -> meta.getMeta()[ 0 ].getDateFormatTimeZone() );
    check( "META_LENIENT_STRING_TO_NUMBER", () -> meta.getMeta()[ 0 ].isLenientStringToNumber() );
    check( "META_DECIMAL", () -> meta.getMeta()[ 0 ].getDecimalSymbol() );
    check( "META_GROUPING", () -> meta.getMeta()[ 0 ].getGroupingSymbol() );
    check( "META_CURRENCY", () -> meta.getMeta()[ 0 ].getCurrencySymbol() );
    check( "META_ENCODING", () -> meta.getMeta()[ 0 ].getEncoding() );

    IValueMeta mftt = new ValueMetaString( "f" );
    injector.setProperty( meta, "META_STORAGE_TYPE", setValue( mftt, "normal" ), "f" );
    assertEquals( 0, meta.getMeta()[ 0 ].getStorageType() );
    injector.setProperty( meta, "META_STORAGE_TYPE", setValue( mftt, "binary-string" ), "f" );
    assertEquals( 1, meta.getMeta()[ 0 ].getStorageType() );
    injector.setProperty( meta, "META_STORAGE_TYPE", setValue( mftt, "indexed" ), "f" );
    assertEquals( 2, meta.getMeta()[ 0 ].getStorageType() );
    skipPropertyTest( "META_STORAGE_TYPE" );

    // TODO check field type plugins
    skipPropertyTest( "META_TYPE" );
  }

  //PDI-16932 test default values length and precision after injection
  @Test
  public void testDefaultValue() throws Exception {
    IValueMeta valueMeta = new ValueMetaString( "f" );
    injector.setProperty( meta, "FIELD_NAME", setValue( valueMeta, "testValue" ), "f" );
    nonTestedProperties.clear(); // we don't need to test other properties
    assertEquals( SelectValuesMeta.UNDEFINED, meta.getSelectFields()[ 0 ].getLength() );
    assertEquals( SelectValuesMeta.UNDEFINED, meta.getSelectFields()[ 0 ].getPrecision() );
  }
}
