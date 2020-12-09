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

package org.apache.hop.pipeline.transforms.salesforceinput;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SalesforceInputDataTest {

  @Test
  public void testConstructor() {
    SalesforceInputData data = new SalesforceInputData();
    assertEquals( 0, data.nr_repeats );
    assertEquals( 0, data.rownr );
    assertNull( data.previousRow );
    assertNull( data.inputRowMeta );
    assertNull( data.outputRowMeta );
    assertNull( data.convertRowMeta );
    assertEquals( 0, data.recordcount );
    assertEquals( 0, data.nrFields );
    assertEquals( false, data.limitReached );
    assertEquals( 0, data.limit );
    assertEquals( 0, data.nrRecords );
    assertEquals( 0, data.recordIndex );
    assertNull( data.startCal );
    assertNull( data.endCal );
    assertEquals( false, data.finishedRecord );
  }
}
