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

package org.apache.hop.core.logging;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
public class LoggingPluginTypeTest {

  private LoggingPlugin annotation;

  @Before
  public void setUp() throws Exception {
    annotation = mock( LoggingPlugin.class );
    when( annotation.id() ).thenReturn( "id" );
  }

  @Test
  public void pickUpsId() throws Exception {
    assertEquals( "id", LoggingPluginType.getInstance().extractID( annotation ) );
  }

  @Test
  public void pickUpName_NameIsSpecified() throws Exception {
    when( annotation.name() ).thenReturn( "name" );
    assertEquals( "name", LoggingPluginType.getInstance().extractName( annotation ) );
  }

  @Test
  public void pickUpName_NameIsNotSpecified() throws Exception {
    assertEquals( "id", LoggingPluginType.getInstance().extractName( annotation ) );
  }
}
