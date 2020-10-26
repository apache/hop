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

package org.apache.hop.pipeline.transforms.xbaseinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.fileinput.BaseParsingTest;
import org.junit.Before;
import org.junit.Ignore;

/**
 * Base class for all XBase input transform tests.
 */
@Ignore( "No tests in abstract base class" )
public class BaseXBaseParsingTest extends BaseParsingTest<XBaseInputMeta, XBaseInputData, XBaseInput> {
  /**
   * Initialize transform info.
   */
  @Before
  public void before() {
    inPrefix = '/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/files/";

    meta = new XBaseInputMeta();
    meta.setDefault();

    data = new XBaseInputData();
    data.outputRowMeta = new RowMeta();
  }

  /**
   * Initialize for processing specified file.
   */
  protected void init( String file ) throws Exception {
    FileObject fo = getFile( file );
    meta.setDbfFileName( fo.getName().getPath() );

    transform = new XBaseInput( transformMeta, null, 1, pipelineMeta, pipeline );
    transform.init();
    transform.addRowListener( rowListener );
  }

  /**
   * For BaseFileInput fields.
   */
  @Override
  protected void setFields( BaseFileField... fields ) throws Exception {
    throw new RuntimeException( "Will be implemented after switch to the BaseFileInput" );
  }
}
