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

import org.apache.hop.databases.cassandra.datastax.DriverCqlRowHandler;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class CassandraOutputTest {

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
  public void validateInvalidTtlFieldTest() {
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
  public void validateEmptyTtlFieldTest() {
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
  public void validateCorrectTtlFieldTest() {
    DriverCqlRowHandler handler = mock(DriverCqlRowHandler.class);

    doCallRealMethod().when(co).validateTtlField(any(), any());
    doCallRealMethod().when(handler).setTtlSec(anyInt());

    String ttl = "120";

    co.validateTtlField(handler, ttl);

    verify(handler, times(1)).setTtlSec(anyInt());
    verify(co, times(0)).logDebug(any());
  }

  @Ignore
  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionNone() {
    String ttlResolveValue = "1"; // none option, this value is ignored default will be -1
    String ttlOption = "None";
    int expectedValue = -1;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }

  @Ignore
  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionSeconds() {
    String ttlResolveValue = "1"; // 1 second
    String ttlOption = "Seconds";
    int expectedValue = 1;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }

  @Ignore
  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionMinutes() {
    String ttlResolveValue = "1"; // 1 minute
    String ttlOption = "Minutes";
    int expectedValue = 60;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }

  @Ignore
  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionHours() {
    String ttlResolveValue = "1"; // 1 hour
    String ttlOption = "Hours";
    int expectedValue = 3600;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }

  @Ignore
  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionDays() {
    String ttlResolveValue = "1"; // 1 day
    String ttlOption = "Days";
    int expectedValue = 86400;

    when(co.resolve(anyString())).thenReturn(ttlResolveValue);
    when(co.getMeta().getTtlUnit()).thenReturn(ttlOption);
    when(co.options.put(anyString(), anyString())).thenReturn("dummy");

    doCallRealMethod().when(co).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify(co.options, times(1)).put(CassandraUtils.BatchOptions.TTL, "" + expectedValue);
  }
}
