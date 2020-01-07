/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.fileinput.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.commons.vfs2.FileObject;
import org.junit.Before;
import org.junit.Test;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.file.BaseFileInputFiles;

public class TextFileInputMetaTest {
  private static final String FILE_NAME_NULL = null;
  private static final String FILE_NAME_EMPTY = StringUtil.EMPTY_STRING;
  private static final String FILE_NAME_VALID_PATH = "path/to/file";

  private TextFileInputMeta inputMeta;
  private VariableSpace variableSpace;

  @Before
  public void setUp() throws Exception {

    TransMeta parentTransMeta = mock( TransMeta.class );

    StepMeta parentStepMeta = mock( StepMeta.class );
    doReturn( parentTransMeta ).when( parentStepMeta ).getParentTransMeta();

    inputMeta = new TextFileInputMeta();
    inputMeta.setParentStepMeta( parentStepMeta );
    inputMeta = spy( inputMeta );
    variableSpace = mock( VariableSpace.class );

    doReturn( "<def>" ).when( variableSpace ).environmentSubstitute( anyString() );
    doReturn( FILE_NAME_VALID_PATH ).when( variableSpace ).environmentSubstitute( FILE_NAME_VALID_PATH );
    FileObject mockedFileObject = mock( FileObject.class );
    doReturn( mockedFileObject ).when( inputMeta ).getFileObject( anyString(), eq( variableSpace ) );
  }

  @Test
  public void testGetXmlWorksIfWeUpdateOnlyPartOfInputFilesInformation() throws Exception {
    inputMeta.inputFiles = new BaseFileInputFiles();
    inputMeta.inputFiles.fileName = new String[] { FILE_NAME_VALID_PATH };

    inputMeta.getXML();

    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.fileMask.length );
    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.excludeFileMask.length );
    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.fileRequired.length );
    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.includeSubFolders.length );
  }

  @Test
  public void testClonelWorksIfWeUpdateOnlyPartOfInputFilesInformation() throws Exception {
    inputMeta.inputFiles = new BaseFileInputFiles();
    inputMeta.inputFiles.fileName = new String[] { FILE_NAME_VALID_PATH };

    TextFileInputMeta cloned = (TextFileInputMeta) inputMeta.clone();

    //since the equals was not override it should be other object
    assertNotEquals( inputMeta, cloned );
    assertEquals( cloned.inputFiles.fileName.length, inputMeta.inputFiles.fileName.length );
    assertEquals( cloned.inputFiles.fileMask.length, inputMeta.inputFiles.fileMask.length );
    assertEquals( cloned.inputFiles.excludeFileMask.length, inputMeta.inputFiles.excludeFileMask.length );
    assertEquals( cloned.inputFiles.fileRequired.length, inputMeta.inputFiles.fileRequired.length );
    assertEquals( cloned.inputFiles.includeSubFolders.length, inputMeta.inputFiles.includeSubFolders.length );

    assertEquals( cloned.inputFields.length, inputMeta.inputFields.length );
    assertEquals( cloned.getFilter().length, inputMeta.getFilter().length );
  }

}
