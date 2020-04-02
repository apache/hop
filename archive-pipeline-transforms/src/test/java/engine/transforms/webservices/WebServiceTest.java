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

package org.apache.hop.pipeline.transforms.webservices;

import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class WebServiceTest {

  private static final String LOCATION_HEADER = "Location";

  private static final String TEST_URL = "TEST_URL";

  private static final String NOT_VALID_URL = "NOT VALID URL";

  private TransformMockHelper<WebServiceMeta, WebServiceData> mockHelper;

  private WebService webServiceTransform;

  @Before
  public void setUpBefore() {
    mockHelper =
      new TransformMockHelper<WebServiceMeta, WebServiceData>( "WebService", WebServiceMeta.class, WebServiceData.class );
    when( mockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      mockHelper.logChannelInterface );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );

    webServiceTransform =
      spy( new WebService( mockHelper.transformMeta, mockHelper.transformDataInterface, 0, mockHelper.pipelineMeta,
        mockHelper.pipeline ) );
  }

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test( expected = URISyntaxException.class )
  public void newHttpMethodWithInvalidUrl() throws URISyntaxException {
    webServiceTransform.getHttpMethod( NOT_VALID_URL );
  }

  @Test
  public void getLocationFrom() {
    HttpPost postMethod = mock( HttpPost.class );
    Header locationHeader = new BasicHeader( LOCATION_HEADER, TEST_URL );
    doReturn( locationHeader ).when( postMethod ).getFirstHeader( LOCATION_HEADER );

    assertEquals( TEST_URL, WebService.getLocationFrom( postMethod ) );
  }

}
