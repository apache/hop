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

package org.apache.hop.pipeline.transforms.getfilesrowscount;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.YNLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetFilesRowsCountMetaTest implements InitializerInterface<ITransform> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;

  @Before
  public void setUp() throws Exception {
    List<String> attributes =
      Arrays.asList( "fileName", "fileMask", "excludeFileMask", "includeFilesCount", "filesCountFieldName",
        "rowsCountFieldName", "RowSeparator_format", "RowSeparator", "filefield", "isaddresult",
        "outputFilenameField", "fileRequired", "includeSubFolders", "smartCount" );
    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "fileName", "getFileName" );
        put( "fileMask", "getFileMask" );
        put( "excludeFileMask", "getExcludeFileMask" );
        put( "includeFilesCount", "includeCountFiles" );
        put( "filesCountFieldName", "getFilesCountFieldName" );
        put( "rowsCountFieldName", "getRowsCountFieldName" );
        put( "RowSeparator_format", "getRowSeparatorFormat" );
        put( "RowSeparator", "getRowSeparator" );
        put( "filefield", "isFileField" );
        put( "isaddresult", "isAddResultFile" );
        put( "outputFilenameField", "getOutputFilenameField" );
        put( "fileRequired", "getFileRequired" );
        put( "includeSubFolders", "getIncludeSubFolders" );
        put( "smartCount", "isSmartCount" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "fileName", "setFileName" );
        put( "fileMask", "setFileMask" );
        put( "excludeFileMask", "setExcludeFileMask" );
        put( "includeFilesCount", "setIncludeCountFiles" );
        put( "filesCountFieldName", "setFilesCountFieldName" );
        put( "rowsCountFieldName", "setRowsCountFieldName" );
        put( "RowSeparator_format", "setRowSeparatorFormat" );
        put( "RowSeparator", "setRowSeparator" );
        put( "filefield", "setFileField" );
        put( "isaddresult", "setAddResultFile" );
        put( "outputFilenameField", "setOutputFilenameField" );
        put( "fileRequired", "setFileRequired" );
        put( "includeSubFolders", "setIncludeSubFolders" );
        put( "smartCount", "setSmartCount" );
      }
    };
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 5 );
    FieldLoadSaveValidator<String[]> ynArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new YNLoadSaveValidator(), 5 );

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "fileName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fileMask", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fileRequired", ynArrayLoadSaveValidator );
    attrValidatorMap.put( "includeSubFolders", ynArrayLoadSaveValidator );
    attrValidatorMap.put( "excludeFileMask", stringArrayLoadSaveValidator );
    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( GetFilesRowsCountMeta.class, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof GetFilesRowsCountMeta ) {
      ( (GetFilesRowsCountMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testLoadSaveXML() throws HopException {
    loadSaveTester.testSerialization();
  }

  // Note - cloneTest() removed as it's now covered by the load/save tester.
}
