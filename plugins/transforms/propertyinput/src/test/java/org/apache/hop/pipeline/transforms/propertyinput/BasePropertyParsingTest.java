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

package org.apache.hop.pipeline.transforms.propertyinput;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.junit.Before;
import org.junit.Ignore;

/**
 * Base class for all CSV input transform tests.
 */
@Ignore( "No tests in abstract base class" )
public class BasePropertyParsingTest extends BaseParsingTest<PropertyInputMeta, PropertyInputData, PropertyInput> {
  /**
   * Initialize transform info.
   */
  @Before
  public void before() {
    meta = new PropertyInputMeta();
    meta.setDefault();

    data = new PropertyInputData();
    data.outputRowMeta = new RowMeta();
  }

  /**
   * Initialize for processing specified file.
   */
  protected void init( String file ) throws Exception {
    meta.setFileName( new String[] { getFile( file ).getURL().getFile() } );
    meta.setFileMask( new String[ 1 ] );
    meta.setExcludeFileMask( new String[ 1 ] );

    transform = new PropertyInput( transformMeta, meta, data, 1, pipelineMeta, pipeline );
    transform.init();
    transform.addRowListener( rowListener );
  }

  /**
   * Declare fields for test.
   */
  protected void setFields( PropertyInputField... fields ) throws Exception {
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
}
