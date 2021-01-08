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

package org.apache.hop.core.plugins;

import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.junit.Test;

import java.io.InputStream;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class BasePluginTypeTest {
//  @Test
//  public void testRegisterNativesCloseResAsStream() throws Exception {
//    BasePluginType bpt = spy( DatabasePluginType.getInstance() );
//
//    InputStream is = mock( InputStream.class );
//
//    doReturn( is ).when( bpt ).getResAsStreamExternal( anyString() );
//    doNothing().when( bpt ).registerPlugins( is );
//
//    bpt.registerNatives();
//
//    verify( is ).close();
//  }
//
//  @Test
//  public void testRegisterNativesCloseFileInStream() throws Exception {
//    BasePluginType bpt = spy( TwoWayPasswordEncoderPluginType.getInstance() );
//
//    InputStream is = mock( InputStream.class );
//
//    doReturn( "foo" ).when( bpt ).getPropertyExternal( anyString(), anyString() );
//    doReturn( null ).when( bpt ).getResAsStreamExternal( anyString() );
//    doReturn( is ).when( bpt ).getFileInputStreamExternal( anyString() );
//    doNothing().when( bpt ).registerPlugins( is );
//
//    bpt.registerNatives();
//
//    verify( is ).close();
//  }
}
