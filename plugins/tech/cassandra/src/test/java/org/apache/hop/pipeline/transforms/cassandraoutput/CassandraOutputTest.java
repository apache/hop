/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hop.pipeline.transforms.cassandraoutput;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.hop.databases.cassandra.datastax.DriverCqlRowHandler;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

class CassandraOutputTest {

  CassandraOutput co;
  CassandraOutputMeta meta;

  @Before
  public void setUp() throws Exception {
    co = mock(CassandraOutput.class);
    meta = mock(CassandraOutputMeta.class);
    co.options = mock(Map.class);
    when(co.getMeta()).thenReturn(meta);
  }

  @Test
  void validateInvalidTtlFieldTest() {
    DriverCqlRowHandler handler = mock(DriverCqlRowHandler.class);
    CassandraOutput co = mock(CassandraOutput.class);

    doCallRealMethod().when(co).validateTtlField(any(), any());
    doCallRealMethod().when(handler).setTtlSec(anyInt());

    String ttl = "a";

    co.validateTtlField(handler, ttl);

    verify(handler, times(0)).setTtlSec(anyInt());
    verify(co, times(1)).logDebug(any());
  }

  @Test
  void validateEmptyTtlFieldTest() {
    DriverCqlRowHandler handler = mock(DriverCqlRowHandler.class);
    CassandraOutput co = mock(CassandraOutput.class);

    doCallRealMethod().when(co).validateTtlField(any(), any());
    doCallRealMethod().when(handler).setTtlSec(anyInt());

    String ttl = "";

    co.validateTtlField(handler, ttl);

    verify(handler, times(0)).setTtlSec(anyInt());
    verify(co, times(0)).logDebug(any());
  }

  @Test
  void validateCorrectTtlFieldTest() {
    DriverCqlRowHandler handler = mock(DriverCqlRowHandler.class);

    doCallRealMethod().when(co).validateTtlField(any(), any());
    doCallRealMethod().when(handler).setTtlSec(anyInt());

    String ttl = "120";

    co.validateTtlField(handler, ttl);

    verify(handler, times(1)).setTtlSec(anyInt());
    verify(co, times(0)).logDebug(any());
  }

  @Ignore("This test needs to be reviewed")
  @Test
  void validateSetTTLIfSpecifiedTestWithOptionNone() {
    String ttlResolveValue = "1"; // none option, this value is ignored default will be -1
    CassandraOutputMeta.TtlUnits ttlOption = CassandraOutputMeta.TtlUnits.NONE;
    int expectedValue = -1;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }

  @Ignore("This test needs to be reviewed")
  @Test
  void validateSetTTLIfSpecifiedTestWithOptionSeconds() {
    String ttlResolveValue = "1"; // 1 second
    CassandraOutputMeta.TtlUnits ttlOption = CassandraOutputMeta.TtlUnits.SECONDS;

    int expectedValue = 1;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }

  @Ignore("This test needs to be reviewed")
  @Test
  void validateSetTTLIfSpecifiedTestWithOptionMinutes() {
    String ttlResolveValue = "1"; // 1 minute
    CassandraOutputMeta.TtlUnits ttlOption = CassandraOutputMeta.TtlUnits.MINUTES;

    int expectedValue = 60;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }

  @Ignore("This test needs to be reviewed")
  @Test
  void validateSetTTLIfSpecifiedTestWithOptionHours() {
    String ttlResolveValue = "1"; // 1 hour
    CassandraOutputMeta.TtlUnits ttlOption = CassandraOutputMeta.TtlUnits.HOURS;

    int expectedValue = 3600;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }

  @Ignore("This test needs to be reviewed")
  @Test
  void validateSetTTLIfSpecifiedTestWithOptionDays() {
    String ttlResolveValue = "1"; // 1 day
    CassandraOutputMeta.TtlUnits ttlOption = CassandraOutputMeta.TtlUnits.DAYS;

    int expectedValue = 86400;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }
}
