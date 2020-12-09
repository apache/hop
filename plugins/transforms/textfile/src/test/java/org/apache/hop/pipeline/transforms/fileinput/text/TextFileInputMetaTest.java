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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputFiles;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class TextFileInputMetaTest {
  private static final String FILE_NAME_NULL = null;
  private static final String FILE_NAME_EMPTY = StringUtil.EMPTY_STRING;
  private static final String FILE_NAME_VALID_PATH = "path/to/file";

  private TextFileInputMeta inputMeta;
  private IVariables variables;

  @Before
  public void setUp() throws Exception {

    PipelineMeta parentPipelineMeta = mock( PipelineMeta.class );

    TransformMeta parentTransformMeta = mock( TransformMeta.class );
    doReturn( parentPipelineMeta ).when( parentTransformMeta ).getParentPipelineMeta();

    inputMeta = new TextFileInputMeta();
    inputMeta.setParentTransformMeta( parentTransformMeta );
    inputMeta = spy( inputMeta );
    variables = mock( IVariables.class );

    doReturn( "<def>" ).when( variables ).resolve( anyString() );
    doReturn( FILE_NAME_VALID_PATH ).when( variables ).resolve( FILE_NAME_VALID_PATH );
    FileObject mockedFileObject = mock( FileObject.class );
    doReturn( mockedFileObject ).when( inputMeta ).getFileObject( anyString(), eq( variables ) );
  }

  @Test
  public void testGetXmlWorksIfWeUpdateOnlyPartOfInputFilesInformation() throws Exception {
    inputMeta.inputFiles = new BaseFileInputFiles();
    inputMeta.inputFiles.fileName = new String[] { FILE_NAME_VALID_PATH };

    inputMeta.getXml();

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
