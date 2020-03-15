/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.job.entries.delay;

import org.apache.hop.core.util.Utils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobEntryDelayTest {

  @Test
  public void testGetRealMaximumTimeout() {
    JobEntryDelay entry = new JobEntryDelay();
    assertTrue( Utils.isEmpty( entry.getRealMaximumTimeout() ) );

    entry.setMaximumTimeout( " 1" );
    assertEquals( "1", entry.getRealMaximumTimeout() );

    entry.setVariable( "testValue", " 20" );
    entry.setMaximumTimeout( "${testValue}" );
    assertEquals( "20", entry.getRealMaximumTimeout() );
  }
}
