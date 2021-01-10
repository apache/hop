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

package org.apache.hop.core.metrics;

import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class MetricsDurationTest {

  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @Test
  @SuppressWarnings( "deprecation" )
  public void test() {
    Date startDate = new Date( ( 2016 - 1900 ), Calendar.JANUARY, 10 );
    Long duration = Long.valueOf( 4L );
    MetricsDuration metric =
      new MetricsDuration( startDate, "theDesc", "theSubj", "theLogChannel", duration );

    assertEquals( "theDesc", metric.getDescription() );
    assertEquals( "theSubj", metric.getSubject() );
    assertEquals( "theLogChannel", metric.getLogChannelId() );
    assertEquals( startDate, metric.getDate() );
    assertEquals( duration, metric.getDuration() );

    assertEquals( Long.valueOf( 1L ), metric.getCount() );
    metric.incrementCount();
    assertEquals( Long.valueOf( 2L ), metric.getCount() );
    assertEquals( new Date( startDate.getTime() + duration ), metric.getEndDate() );
  }
}
