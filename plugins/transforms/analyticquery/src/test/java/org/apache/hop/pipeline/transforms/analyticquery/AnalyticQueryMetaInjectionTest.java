/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.analyticquery;


import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class AnalyticQueryMetaInjectionTest extends BaseMetadataInjectionTest<AnalyticQueryMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new AnalyticQueryMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "GROUP_FIELDS", () -> meta.getGroupField()[ 0 ] );
    check( "OUTPUT.AGGREGATE_FIELD", () -> meta.getAggregateField()[ 0 ] );
    check( "OUTPUT.SUBJECT_FIELD", () -> meta.getSubjectField()[ 0 ] );
    check( "OUTPUT.AGGREGATE_TYPE", () -> meta.getAggregateType()[ 0 ] );
    check( "OUTPUT.VALUE_FIELD", () -> meta.getValueField()[ 0 ] );
  }
}
