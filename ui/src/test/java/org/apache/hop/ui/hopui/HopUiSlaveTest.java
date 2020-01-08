/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.ui.hopui;

import org.apache.hop.core.exception.HopException;
import org.apache.http.client.ClientProtocolException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class HopUiSlaveTest {


  @Before
  public void setup() {

  }

  @Test
  public void setErrorTextWithNoCauseException() {
    Exception e = new HopException( "kettleMessage" );

    HopUiSlave hopUiSlave = mock( HopUiSlave.class );
    doCallRealMethod().when( hopUiSlave ).setExceptionMessage( any( Exception.class ) );

    String message = hopUiSlave.setExceptionMessage( e );

    assertEquals( message, e.getMessage().toString() );

  }

  @Test
  public void setErrorTextWithCauseMessageException() {
    ClientProtocolException cpe = new ClientProtocolException( "causeMessage" );
    Exception e = new HopException( "kettleMessage", cpe );

    HopUiSlave hopUiSlave = mock( HopUiSlave.class );
    doCallRealMethod().when( hopUiSlave ).setExceptionMessage( any( Exception.class ) );

    String message = hopUiSlave.setExceptionMessage( e );

    Throwable cause = e.getCause();

    assertEquals( message, cause.getMessage().toString() );

  }

  @Test
  public void setErrorTextWithCauseExceptionWithoutCauseMessage() {
    //cause without message
    ClientProtocolException cpe = new ClientProtocolException();
    Exception e = new HopException( "kettleMessage", cpe );

    HopUiSlave hopUiSlave = mock( HopUiSlave.class );
    doCallRealMethod().when( hopUiSlave ).setExceptionMessage( any( Exception.class ) );

    String message = hopUiSlave.setExceptionMessage( e );

    assertEquals( message, e.getMessage().toString() );

  }

}
