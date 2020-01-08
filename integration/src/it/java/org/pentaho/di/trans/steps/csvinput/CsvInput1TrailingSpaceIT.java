/*! ****************************************************************************
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

package org.apache.hop.trans.steps.csvinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Regression test case for PDI JIRA-1317: a csv input step with less columns in certain rows than the number of columns
 * defined in the step.
 * <p>
 * In the original problem (in v3.1-M2) this caused the filename column to be in the wrong places.
 *
 * @author Sven Boden Modified by Sean Flatley
 */
public class CsvInput1TrailingSpaceIT extends CsvInput1IT {

  /**
   * Write the file to be used as input (as a temporary file).
   *
   * @return Absolute file name/path of the created file.
   * @throws IOException UPON
   */
  public String writeInputFile() throws IOException {

    String rcode = null;

    File tempFile = File.createTempFile( "PDI_tmp", ".tmp" );
    tempFile.deleteOnExit();

    rcode = tempFile.getAbsolutePath();

    FileWriter fout = new FileWriter( tempFile );
    fout.write( "A;B;C;D;E\n" );
    fout.write( "1;'b0' ;'c0' \n" );
    fout.write( "2;'b1' ;'c1' ;'d1' ;'e1' \n" );
    fout.write( "3;'b2' ;'c2' \n" );

    fout.close();

    return rcode;
  }

  /**
   * Test case for Get XML Data step, very simple example.
   *
   * @throws Exception Upon any exception
   */
  @Test
  public void testCSVInput1() throws Exception {
    HopEnvironment.init();

    //
    // Create a new transformation...
    //
    TransMeta transMeta = new TransMeta();
    transMeta.setName( "csvinput1" );

    PluginRegistry registry = PluginRegistry.getInstance();

    String fileName = writeInputFile();

    StepMeta injectorStep = createInjectorStep( transMeta, registry );
    StepMeta csvInputStep = createCsvInputStep( transMeta, registry, "\'", true );

    createAndTestTrans(
      registry, transMeta, injectorStep, csvInputStep, fileName, createTextFileInputFields().length );
  }
}
