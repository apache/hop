/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
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

package org.apache.hop.pipeline.transforms.simplemapping;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.PDI_11948_TransformsTestsParent;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The PDI_11948_SimpleMappingTest class tests Simple Mapping transform of PDI-11948 bug. It's check if HttpServletResponse
 * object is null and call or not setServletReponse( HttpServletResponse response ) method of appropriate Pipeline object.
 *
 * @author Yury Bakhmutski
 * @see org.apache.hop.pipeline.transforms.simplemapping.SimpleMapping
 */
public class PDI_11948_SimpleMappingTest extends PDI_11948_TransformsTestsParent<SimpleMapping, SimpleMappingData> {

  @Override
  @Before
  public void init() throws Exception {
    super.init();
    transformMock = mock( SimpleMapping.class );
    transformDataMock = mock( SimpleMappingData.class );
  }

  @Test
  public void testSimpleMappingTransform() throws HopException {

    when( transformMock.getData() ).thenReturn( transformDataMock );
    when( transformDataMock.getMappingPipeline() ).thenReturn( transMock );

    // stubbing methods for null-checking
    when( transformMock.getPipeline() ).thenReturn( transMock );
    when( transMock.getServletResponse() ).thenReturn( null );

    doThrow( new RuntimeException( "The getServletResponse() mustn't be executed!" ) ).when( transMock )
      .setServletReponse( any( HttpServletResponse.class ) );

    doCallRealMethod().when( transformMock ).initServletConfig();
    transformMock.initServletConfig();
  }
}
