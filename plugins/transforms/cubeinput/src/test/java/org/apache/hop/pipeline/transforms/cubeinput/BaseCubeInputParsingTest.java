/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.cubeinput;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.junit.Before;
import org.junit.Ignore;

/**
 * Base class for all Cube Input transform tests.
 */
@Ignore( "No tests in abstract base class" )
public class BaseCubeInputParsingTest extends BaseParsingTest<CubeInputMeta, CubeInputData, CubeInput> {
  /**
   * Initialize transform info.
   */
  @Before
  public void before() {
    meta = new CubeInputMeta();
    meta.setDefault();

    data = new CubeInputData();
    data.meta = new RowMeta();
  }

  /**
   * Initialize for processing specified file.
   */
  protected void init( String file ) throws Exception {
    meta.setFilename( getFile( file ).getURL().getFile() );

    transform = new CubeInput( transformMeta, meta, null, 1, pipelineMeta, pipeline );
    transform.init();
    transform.addRowListener( rowListener );
  }

  /**
   * For BaseFileInput fields.
   */
  @Override
  protected void setFields( BaseFileField... fields ) throws Exception {
    throw new RuntimeException( "Not implemented" );
  }

  /**
   * CSV input transform produces byte arrays instead strings.
   */
  @Override
  protected void check( Object[][] expected ) throws Exception {
    for ( int r = 0; r < expected.length; r++ ) {
      for ( int c = 0; c < expected[ r ].length; c++ ) {
        expected[ r ][ c ] = expected[ r ][ c ].toString().getBytes( "UTF-8" );
      }
    }
    super.check( expected );
  }
}
