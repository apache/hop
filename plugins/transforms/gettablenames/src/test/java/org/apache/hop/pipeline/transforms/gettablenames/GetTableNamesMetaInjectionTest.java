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
package org.apache.hop.pipeline.transforms.gettablenames;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class GetTableNamesMetaInjectionTest extends BaseMetadataInjectionTest<GetTableNamesMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new GetTableNamesMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SCHEMANAME", () -> meta.getSchemaName() );
    check( "TABLENAMEFIELDNAME", () -> meta.getTablenameFieldName() );
    check( "SQLCREATIONFIELDNAME", () -> meta.getSqlCreationFieldName() );
    check( "OBJECTTYPEFIELDNAME", () -> meta.getObjectTypeFieldName() );
    check( "ISSYSTEMOBJECTFIELDNAME", () -> meta.isSystemObjectFieldName() );
    check( "INCLUDECATALOG", () -> meta.isIncludeCatalog() );
    check( "INCLUDESCHEMA", () -> meta.isIncludeSchema() );
    check( "INCLUDETABLE", () -> meta.isIncludeTable() );
    check( "INCLUDEVIEW", () -> meta.isIncludeView() );
    check( "INCLUDEPROCEDURE", () -> meta.isIncludeProcedure() );
    check( "INCLUDESYNONYM", () -> meta.isIncludeSynonym() );
    check( "ADDSCHEMAINOUTPUT", () -> meta.isAddSchemaInOut() );
    check( "DYNAMICSCHEMA", () -> meta.isDynamicSchema() );
    check( "SCHEMANAMEFIELD", () -> meta.getSchemaFieldName() );
    check( "CONNECTIONNAME", () -> "My Connection", "My Connection" );
  }
}
