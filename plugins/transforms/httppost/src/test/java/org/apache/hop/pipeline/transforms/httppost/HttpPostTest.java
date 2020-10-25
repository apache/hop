/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.httppost;

import org.apache.hop.core.exception.HopException;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class HttpPostTest {

  @Test
  public void getRequestBodyParametersAsStringWithNullEncoding() throws HopException {
    HttpPost http = mock( HttpPost.class );
    doCallRealMethod().when( http ).getRequestBodyParamsAsStr( any( NameValuePair[].class ), anyString() );

    NameValuePair[] pairs = new NameValuePair[] {
      new BasicNameValuePair( "u", "usr" ),
      new BasicNameValuePair( "p", "pass" )
    };

    assertEquals( "u=usr&p=pass", http.getRequestBodyParamsAsStr( pairs, null ) );
  }

}
