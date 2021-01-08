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

package org.apache.hop.pipeline.transforms.getvariable;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class GetVariableMetaInjectionTest extends BaseMetadataInjectionTest<GetVariableMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new GetVariableMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FIELDNAME", () -> meta.getFieldDefinitions()[ 0 ].getFieldName() );
    check( "VARIABLE", () -> meta.getFieldDefinitions()[ 0 ].getVariableString() );
    check( "FIELDTYPE", () -> meta.getFieldDefinitions()[ 0 ].getFieldType() );
    check( "FIELDFORMAT", () -> meta.getFieldDefinitions()[ 0 ].getFieldFormat() );
    check( "FIELDLENGTH", () -> meta.getFieldDefinitions()[ 0 ].getFieldLength() );
    check( "FIELDPRECISION", () -> meta.getFieldDefinitions()[ 0 ].getFieldPrecision() );
    check( "CURRENCY", () -> meta.getFieldDefinitions()[ 0 ].getCurrency() );
    check( "DECIMAL", () -> meta.getFieldDefinitions()[ 0 ].getDecimal() );
    check( "GROUP", () -> meta.getFieldDefinitions()[ 0 ].getGroup() );
    check( "TRIMTYPE", () -> meta.getFieldDefinitions()[ 0 ].getTrimType() );
  }
}
