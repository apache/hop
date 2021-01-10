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

package org.apache.hop.workflow.actions.delay;

import org.apache.hop.core.util.Utils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WorkflowEntryDelayTest {

  @Test
  public void testGetRealMaximumTimeout() {
    ActionDelay entry = new ActionDelay();
    assertTrue( Utils.isEmpty( entry.getRealMaximumTimeout() ) );

    entry.setMaximumTimeout( " 1" );
    assertEquals( "1", entry.getRealMaximumTimeout() );

    entry.setVariable( "testValue", " 20" );
    entry.setMaximumTimeout( "${testValue}" );
    assertEquals( "20", entry.getRealMaximumTimeout() );
  }
}
