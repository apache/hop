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

package org.apache.hop.pipeline.transforms.csvinput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
//import org.apache.hop.pipeline.transforms.fileinput.BaseParsingTest;
import org.apache.hop.core.file.TextFileInputField;
import org.junit.Before;
import org.junit.Ignore;

/**
 * Base class for all CSV input transform tests.
 */
@Ignore( "No tests in abstract base class" )
public abstract class BaseCsvParsingTest extends BaseParsingTest<CsvInputMeta, CsvInputData, CsvInput> {
  /**
   * Initialize transform info.
   */
  @Before
  public void before() {
    meta = new CsvInputMeta();
    meta.setDefault();

    data = new CsvInputData();
    data.outputRowMeta = new RowMeta();
  }

  /**
   * Initialize for processing specified file.
   */
  protected void init( String file ) throws Exception {
    init( file, false );
  }

  protected void init( String file, boolean absolutePath ) throws Exception {

    if ( absolutePath ) {
      meta.setFilename( file );
    } else {
      meta.setFilename( getFile( file ).getURL().getFile() );
    }

    transform = new CsvInput( transformMeta, meta, data, 1, pipelineMeta, pipeline );
    transform.init();
    transform.addRowListener( rowListener );
  }

  /**
   * Declare fields for test.
   */
  protected void setFields( TextFileInputField... fields ) throws Exception {
    meta.setInputFields( fields );
    meta.getFields( data.outputRowMeta, meta.getName(), null, null, new Variables(), null );
    data.convertRowMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );
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
      if ( expected[ r ].length != 0 ) {
        for ( int c = 0; c < expected[ r ].length; c++ ) {
          if ( expected[ r ][ c ] == "" ) {
            expected[ r ][ c ] = StringUtils.EMPTY.getBytes( "UTF-8" );
          } else if ( expected[ r ][ c ] == null ) {
            expected[ r ][ c ] = null;
          } else {
            expected[ r ][ c ] = expected[ r ][ c ].toString().getBytes( "UTF-8" );
          }
        }
      } else {
        expected[ r ] = new Object[ meta.getInputFields().length ];
        expected[ r ][ 0 ] = StringUtils.EMPTY.getBytes( "UTF-8" );
      }
    }
    super.check( expected );
  }
}
