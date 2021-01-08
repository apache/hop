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

package org.apache.hop.pipeline.transforms.exceloutput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ExcelOutputMetaInjectionTest extends BaseMetadataInjectionTest<ExcelOutputMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new ExcelOutputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "HEADER_FONT_SIZE", () -> meta.getHeaderFontSize() );
    check( "HEADER_FONT_BOLD", () -> meta.isHeaderFontBold() );
    check( "HEADER_FONT_ITALIC", () -> meta.isHeaderFontItalic() );
    check( "HEADER_FONT_COLOR", () -> meta.getHeaderFontColor() );
    check( "HEADER_BACKGROUND_COLOR", () -> meta.getHeaderBackGroundColor() );
    check( "HEADER_ROW_HEIGHT", () -> Integer.parseInt( meta.getHeaderRowHeight() ) );
    check( "HEADER_IMAGE", () -> meta.getHeaderImage() );
    check( "ROW_FONT_SIZE", () -> meta.getRowFontSize() );
    check( "ROW_FONT_COLOR", () -> meta.getRowFontColor() );
    check( "ROW_BACKGROUND_COLOR", () -> meta.getRowBackGroundColor() );

    check( "FILENAME", () -> meta.getFileName() );
    check( "EXTENSION", () -> meta.getExtension() );
    check( "PASSWORD", () -> meta.getPassword() );

    check( "HEADER_ENABLED", () -> meta.isHeaderEnabled() );
    check( "FOOTER_ENABLED", () -> meta.isFooterEnabled() );
    check( "SPLIT_EVERY", () -> meta.getSplitEvery() );
    check( "TRANSFORM_NR_IN_FILENAME", () -> meta.isTransformNrInFilename() );
    check( "DATE_IN_FILENAME", () -> meta.isDateInFilename() );
    check( "FILENAME_TO_RESULT", () -> meta.isAddToResultFiles() );
    check( "PROTECT", () -> meta.isSheetProtected() );
    check( "TIME_IN_FILENAME", () -> meta.isTimeInFilename() );
    check( "TEMPLATE", () -> meta.isTemplateEnabled() );
    check( "TEMPLATE_FILENAME", () -> meta.getTemplateFileName() );
    check( "TEMPLATE_APPEND", () -> meta.isTemplateAppend() );
    check( "SHEET_NAME", () -> meta.getSheetname() );
    check( "USE_TEMPFILES", () -> meta.isUseTempFiles() );
    check( "TEMPDIR", () -> meta.getTempDirectory() );

    check( "NAME", () -> meta.getOutputFields()[ 0 ].getName() );
    // TODO check field type plugins
    skipPropertyTest( "TYPE" );

    check( "FORMAT", () -> meta.getOutputFields()[ 0 ].getFormat() );

    check( "ENCODING", () -> meta.getEncoding() );
    check( "NEWLINE", () -> meta.getNewline() );
    check( "APPEND", () -> meta.isAppend() );
    check( "DONT_OPEN_NEW_FILE", () -> meta.isDoNotOpenNewFileInit() );
    check( "CREATE_PARENT_FOLDER", () -> meta.isCreateParentFolder() );
    check( "DATE_FORMAT_SPECIFIED", () -> meta.isSpecifyFormat() );
    check( "DATE_FORMAT", () -> meta.getDateTimeFormat() );
    check( "AUTOSIZE_COLUMNS", () -> meta.isAutoSizeColums() );
    check( "NULL_AS_BLANK", () -> meta.isNullBlank() );

    checkStringToInt( "HEADER_FONT_NAME", () -> meta.getHeaderFontName(), new String[] { "arial", "courier", "tahoma", "times" }, new int[] { 0, 1, 2, 3 } );

    checkStringToInt( "ROW_FONT_NAME", () -> meta.getRowFontName(), new String[] { "arial", "courier", "tahoma", "times" }, new int[] { 0, 1, 2, 3 } );

    checkStringToInt( "HEADER_FONT_UNDERLINE", () -> meta.getHeaderFontUnderline(), new String[] { "no", "single", "single_accounting", "double", "double_accounting" }, new int[] { 0, 1, 2, 3,
      4 } );
    checkStringToInt( "HEADER_FONT_ORIENTATION", () -> meta.getHeaderFontOrientation(), new String[] { "horizontal", "minus_45", "minus_90", "plus_45", "plus_90", "stacked", "vertical" }, new int[] {
      0, 1, 2, 3, 4, 5, 6 } );
    checkStringToInt( "HEADER_ALIGNMENT", () -> meta.getHeaderAlignment(), new String[] { "left", "right", "center", "fill", "general", "justify" }, new int[] { 0, 1, 2, 3, 4, 5 } );
  }
}
