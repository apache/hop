/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.cassandraoutput;

import org.junit.Test;
import org.pentaho.cassandra.driver.datastax.DriverCQLRowHandler;
import org.pentaho.cassandra.util.CassandraUtils;


import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class CassandraOutputTest {

  @Test
  public void validateInvalidTtlFieldTest() {
    DriverCQLRowHandler handler = mock( DriverCQLRowHandler.class );
    CassandraOutput co = mock( CassandraOutput.class );

    doCallRealMethod().when( co ).validateTtlField( any(), any() );
    doCallRealMethod().when( handler ).setTtlSec( anyInt() );

    String ttl = "a";

    co.validateTtlField( handler, ttl );

    verify( handler, times(0) ).setTtlSec( anyInt() );
    verify( co, times(1) ).logDebug( any() );
  }

  @Test
  public void validateEmptyTtlFieldTest() {
    DriverCQLRowHandler handler = mock( DriverCQLRowHandler.class );
    CassandraOutput co = mock( CassandraOutput.class );

    doCallRealMethod().when( co ).validateTtlField( any(), any() );
    doCallRealMethod().when( handler ).setTtlSec( anyInt() );

    String ttl = "";

    co.validateTtlField( handler, ttl );

    verify( handler, times(0) ).setTtlSec( anyInt() );
    verify( co, times(0) ).logDebug( any() );
  }

  @Test
  public void validateCorrectTtlFieldTest() {
    DriverCQLRowHandler handler = mock( DriverCQLRowHandler.class );
    CassandraOutput co = mock( CassandraOutput.class );

    doCallRealMethod().when( co ).validateTtlField( any(), any() );
    doCallRealMethod().when( handler ).setTtlSec( anyInt() );

    String ttl = "120";

    co.validateTtlField( handler, ttl );

    verify( handler, times(1) ).setTtlSec( anyInt() );
    verify( co, times(0) ).logDebug( any() );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionNone() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );


    String ttlEnvironmentSubstituteValue= "1"; // none option, this value is ignored default will be -1
    String ttlOption =  "None";
    int expectedValue = -1;

    when( co.environmentSubstitute( anyString() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( anyString(), anyString()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionSeconds() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );

    String ttlEnvironmentSubstituteValue= "1"; // 1 second
    String ttlOption =  "Seconds";
    int expectedValue = 1;

    when( co.environmentSubstitute( anyString() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( anyString(), anyString()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionMinutes() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );

    String ttlEnvironmentSubstituteValue= "1"; //1 minute
    String ttlOption =  "Minutes";
    int expectedValue = 60;

    when( co.environmentSubstitute( anyString() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( anyString(), anyString()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionHours() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );

    String ttlEnvironmentSubstituteValue= "1"; //1 hour
    String ttlOption =  "Hours";
    int expectedValue = 3600;

    when( co.environmentSubstitute( anyString() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( anyString(), anyString()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionDays() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );

    String ttlEnvironmentSubstituteValue= "1"; // 1 day
    String ttlOption =  "Days";
    int expectedValue = 86400;

    when( co.environmentSubstitute( anyString() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( anyString(), anyString()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }


}
