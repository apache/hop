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

package org.apache.hop.pipeline.transforms.fixedinput;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.fileinput.BaseParsingTest;
import org.junit.Before;
import org.junit.Ignore;

/**
 * Base class for all Fixed input transform tests.
 */
@Ignore( "No tests in abstract base class" )
public class BaseFixedParsingTest extends BaseParsingTest<FixedInputMeta, FixedInputData, FixedInput> {
  /**
   * Initialize transform info.
   */
  @Before
  public void before() {
    meta = new FixedInputMeta();
    meta.setDefault();

    data = new FixedInputData();
    data.outputRowMeta = new RowMeta();
  }

  /**
   * Initialize for processing specified file.
   */
  protected void init( String file ) throws Exception {
    meta.setFilename( getFile( file ).getURL().getFile() );

    transform = new FixedInput( transformMeta, null, 1, pipelineMeta, pipeline );
    transform.init( meta, data );
    transform.addRowListener( rowListener );
  }

  /**
   * Declare fields for test.
   */
  protected void setFields( FixedFileInputField... fields ) throws Exception {
    meta.setFieldDefinition( fields );
    meta.getFields( data.outputRowMeta, meta.getName(), null, null, new Variables(), null );
    data.convertRowMeta = data.outputRowMeta.cloneToType( ValueMetaInterface.TYPE_STRING );
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
