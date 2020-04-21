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

package org.apache.hop.pipeline.transforms.propertyoutput;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PropertyOutputIT {

  @Before
  public void setUp() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.addPluginType( TransformPluginType.getInstance() );
    PluginRegistry.init();
    if ( !Props.isInitialized() ) {
      Props.init();
    }
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testExecute() throws HopException, IOException {
    PipelineMeta meta = new PipelineMeta( getClass().getResource( "propertyOutput.hpl" ).getPath() );
    Pipeline pipeline = new LocalPipelineEngine( meta );
    pipeline.execute();
    pipeline.waitUntilFinished();

    //check that pipeline is finished
    assertTrue( pipeline.isFinished() );

    PropertyOutputData dataTransform = (PropertyOutputData) pipeline.getTransforms().get( 1 ).data;

    RandomAccessFile fos = null;
    try {
      File file = new File( URI.create( dataTransform.filename.replace( "\\", "/" ) ).getPath() );
      if ( file.exists() ) {
        fos = new RandomAccessFile( file, "rw" );
      }
    } catch ( FileNotFoundException | SecurityException e ) {
      fail( "the file with properties should be unallocated" );
    } finally {
      if ( fos != null ) {
        fos.close();
      }
    }
  }

}
