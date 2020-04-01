/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2016-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.steps.fileinput.text;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.steps.file.BaseFileField;
import org.apache.hop.pipeline.steps.fileinput.BaseParsingTest;
import org.junit.Before;
import org.junit.Ignore;

/**
 * Base class for all TextFileInput step tests.
 */
@Ignore( "No tests in abstract base class" )
public abstract class BaseTextParsingTest extends BaseParsingTest<TextFileInputMeta, TextFileInputData, TextFileInput> {
  /**
   * Initialize step info.
   */
  @Before
  public void before() {
    meta = new TextFileInputMeta();
    meta.setDefault();
    stepMeta.setStepMetaInterface( meta );

    data = new TextFileInputData();
    data.outputRowMeta = new RowMeta();
  }

  /**
   * Initialize for processing specified file.
   */
  protected void initByFile( String file ) throws Exception {
    initByURL( getFile( file ).getURL().getFile() );
  }

  /**
   * Initialize for processing specified file by URL.
   */
  protected void initByURL( String url ) throws Exception {
    meta.inputFiles.fileName = new String[] { url };
    meta.inputFiles.fileMask = new String[] { null };
    meta.inputFiles.excludeFileMask = new String[] { null };
    meta.inputFiles.fileRequired = new String[] { "Y" };
    meta.inputFiles.includeSubFolders = new String[] { "N" };

    step = new TextFileInput( stepMeta, null, 1, pipelineMeta, pipeline );
    step.init( meta, data );
    step.addRowListener( rowListener );
  }

  /**
   * Declare fields for test.
   * <p>
   * TODO: move to BaseParsingTest after CSV moving to BaseFileInput
   */
  protected void setFields( BaseFileField... fields ) throws Exception {
    meta.inputFields = fields;
    meta.getFields( data.outputRowMeta, meta.getName(), null, null, new Variables(), null );
    data.convertRowMeta = data.outputRowMeta.cloneToType( ValueMetaInterface.TYPE_STRING );
  }
}
