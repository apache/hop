/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ModPartitionerTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testSerialization() throws HopException {
    List<String> attributes = Arrays.asList( "FieldName" );
    LoadSaveTester<ModPartitioner> tester = new LoadSaveTester<ModPartitioner>( ModPartitioner.class, attributes );

    tester.testSerialization();
  }
}
