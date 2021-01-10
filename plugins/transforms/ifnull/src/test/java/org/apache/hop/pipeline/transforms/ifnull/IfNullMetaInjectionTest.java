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

package org.apache.hop.pipeline.transforms.ifnull;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class IfNullMetaInjectionTest extends BaseMetadataInjectionTest<IfNullMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new IfNullMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FIELD_NAME", () -> meta.getFields()[ 0 ].getFieldName() );
    check( "REPLACE_VALUE", () -> meta.getFields()[ 0 ].getReplaceValue() );
    check( "TYPE_NAME", () -> meta.getValueTypes()[ 0 ].getTypeName() );
    check( "TYPE_REPLACE_VALUE", () -> meta.getValueTypes()[ 0 ].getTypereplaceValue() );
    check( "TYPE_REPLACE_MASK", () -> meta.getValueTypes()[ 0 ].getTypereplaceMask() );
    check( "REPLACE_MASK", () -> meta.getFields()[ 0 ].getReplaceMask() );
    check( "SET_TYPE_EMPTY_STRING", () -> meta.getValueTypes()[ 0 ].isSetTypeEmptyString() );
    check( "SET_EMPTY_STRING", () -> meta.getFields()[ 0 ].isSetEmptyString() );
    check( "SELECT_FIELDS", () -> meta.isSelectFields() );
    check( "SELECT_VALUES_TYPE", () -> meta.isSelectValuesType() );
    check( "REPLACE_ALL_BY_VALUE", () -> meta.getReplaceAllByValue() );
    check( "REPLACE_ALL_MASK", () -> meta.getReplaceAllMask() );
    check( "SET_EMPTY_STRING_ALL", () -> meta.isSetEmptyStringAll() );
  }

}
