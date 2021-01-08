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

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TextFileInputMetaNewInjectionTest extends BaseMetadataInjectionTest<TextFileInputMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new TextFileInputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FILE_TYPE", () -> meta.content.fileType );

    check( "SEPARATOR", () -> meta.content.separator );

    check( "ENCLOSURE", () -> meta.content.enclosure );

    check( "BREAK_IN_ENCLOSURE", () -> meta.content.breakInEnclosureAllowed );

    check( "ESCAPE_CHAR", () -> meta.content.escapeCharacter );

    check( "HEADER_PRESENT", () -> meta.content.header );

    check( "NR_HEADER_LINES", () -> meta.content.nrHeaderLines );

    check( "HAS_FOOTER", () -> meta.content.footer );

    check( "NR_FOOTER_LINES", () -> meta.content.nrFooterLines );

    check( "HAS_WRAPPED_LINES", () -> meta.content.lineWrapped );

    check( "NR_WRAPS", () -> meta.content.nrWraps );

    check( "HAS_PAGED_LAYOUT", () -> meta.content.layoutPaged );

    check( "NR_LINES_PER_PAGE", () -> meta.content.nrLinesPerPage );

    check( "NR_DOC_HEADER_LINES", () -> meta.content.nrLinesDocHeader );

    check( "COMPRESSION_TYPE", () -> meta.content.fileCompression );

    check( "NO_EMPTY_LINES", () -> meta.content.noEmptyLines );

    check( "INCLUDE_FILENAME", () -> meta.content.includeFilename );

    check( "FILENAME_FIELD", () -> meta.content.filenameField );

    check( "INCLUDE_ROW_NUMBER", () -> meta.content.includeRowNumber );

    check( "ROW_NUMBER_FIELD", () -> meta.content.rowNumberField );

    check( "ROW_NUMBER_BY_FILE", () -> meta.content.rowNumberByFile );

    check( "FILE_FORMAT", () -> meta.content.fileFormat );

    check( "ENCODING", () -> meta.content.encoding );

    check( "LENGTH", () -> meta.content.length );

    check( "ROW_LIMIT", () -> meta.content.rowLimit );

    check( "DATE_FORMAT_LENIENT", () -> meta.content.dateFormatLenient );

    check( "DATE_FORMAT_LOCALE", () -> meta.content.dateFormatLocale.toString(), "en", "en_us" );

    ///////////////////////////////
    check( "FILTER_POSITION", () -> meta.getFilter()[ 0 ].getFilterPosition() );

    check( "FILTER_STRING", () -> meta.getFilter()[ 0 ].getFilterString() );

    check( "FILTER_LAST_LINE", () -> meta.getFilter()[ 0 ].isFilterLastLine() );

    check( "FILTER_POSITIVE", () -> meta.getFilter()[ 0 ].isFilterPositive() );

    ///////////////////////////////
    check( "FILENAME", () -> meta.inputFiles.fileName[ 0 ] );

    check( "FILEMASK", () -> meta.inputFiles.fileMask[ 0 ] );

    check( "EXCLUDE_FILEMASK", () -> meta.inputFiles.excludeFileMask[ 0 ] );

    check( "FILE_REQUIRED", () -> meta.inputFiles.fileRequired[ 0 ] );

    check( "INCLUDE_SUBFOLDERS", () -> meta.inputFiles.includeSubFolders[ 0 ] );

    check( "ACCEPT_FILE_NAMES", () -> meta.inputFiles.acceptingFilenames );

    check( "ACCEPT_FILE_TRANSFORM", () -> meta.inputFiles.acceptingTransformName );

    check( "PASS_THROUGH_FIELDS", () -> meta.inputFiles.passingThruFields );

    check( "ACCEPT_FILE_FIELD", () -> meta.inputFiles.acceptingField );

    check( "ADD_FILES_TO_RESULT", () -> meta.inputFiles.isaddresult );

    /////////////////////////////
    check( "FIELD_NAME", () -> meta.inputFields[ 0 ].getName() );

    check( "FIELD_POSITION", () -> meta.inputFields[ 0 ].getPosition() );

    check( "FIELD_LENGTH", () -> meta.inputFields[ 0 ].getLength() );

    // TODO check field type plugins
    // IValueMeta mft = new ValueMetaString( "f" );
    // ValueMetaFactory.createValueMeta( "INTEGER", 5 );
    // injector.setProperty( meta, "FIELD_TYPE", setValue( mft, "INTEGER" ), "f" );
    // assertEquals( 5, meta.inputFiles.inputFields[0].getType() );
    skipPropertyTest( "FIELD_TYPE" );

    check( "FIELD_IGNORE", () -> meta.inputFields[ 0 ].isIgnored() );

    check( "FIELD_FORMAT", () -> meta.inputFields[ 0 ].getFormat() );

    IValueMeta mftt = new ValueMetaString( "f" );
    injector.setProperty( meta, "FIELD_TRIM_TYPE", setValue( mftt, "left" ), "f" );
    assertEquals( 1, meta.inputFields[ 0 ].getTrimType() );
    injector.setProperty( meta, "FIELD_TRIM_TYPE", setValue( mftt, "right" ), "f" );
    assertEquals( 2, meta.inputFields[ 0 ].getTrimType() );
    skipPropertyTest( "FIELD_TRIM_TYPE" );

    check( "FIELD_PRECISION", () -> meta.inputFields[ 0 ].getPrecision() );

    check( "FIELD_CURRENCY", () -> meta.inputFields[ 0 ].getCurrencySymbol() );

    check( "FIELD_DECIMAL", () -> meta.inputFields[ 0 ].getDecimalSymbol() );

    check( "FIELD_GROUP", () -> meta.inputFields[ 0 ].getGroupSymbol() );

    check( "FIELD_REPEAT", () -> meta.inputFields[ 0 ].isRepeated() );

    check( "FIELD_NULL_STRING", () -> meta.inputFields[ 0 ].getNullString() );

    check( "FIELD_IF_NULL", () -> meta.inputFields[ 0 ].getIfNullValue() );

    ///////////////////////////////
    check( "IGNORE_ERRORS", () -> meta.errorHandling.errorIgnored );

    check( "FILE_ERROR_FIELD", () -> meta.errorHandling.fileErrorField );

    check( "FILE_ERROR_MESSAGE_FIELD", () -> meta.errorHandling.fileErrorMessageField );

    check( "SKIP_BAD_FILES", () -> meta.errorHandling.skipBadFiles );

    check( "WARNING_FILES_TARGET_DIR", () -> meta.errorHandling.warningFilesDestinationDirectory );

    check( "WARNING_FILES_EXTENTION", () -> meta.errorHandling.warningFilesExtension );

    check( "ERROR_FILES_TARGET_DIR", () -> meta.errorHandling.errorFilesDestinationDirectory );

    check( "ERROR_FILES_EXTENTION", () -> meta.errorHandling.errorFilesExtension );

    check( "LINE_NR_FILES_TARGET_DIR", () -> meta.errorHandling.lineNumberFilesDestinationDirectory );

    check( "LINE_NR_FILES_EXTENTION", () -> meta.errorHandling.lineNumberFilesExtension );

    //////////////////////
    check( "FILE_SHORT_FILE_FIELDNAME", () -> meta.additionalOutputFields.shortFilenameField );
    check( "FILE_EXTENSION_FIELDNAME", () -> meta.additionalOutputFields.extensionField );
    check( "FILE_PATH_FIELDNAME", () -> meta.additionalOutputFields.pathField );
    check( "FILE_SIZE_FIELDNAME", () -> meta.additionalOutputFields.sizeField );
    check( "FILE_HIDDEN_FIELDNAME", () -> meta.additionalOutputFields.hiddenField );
    check( "FILE_LAST_MODIFICATION_FIELDNAME", () -> meta.additionalOutputFields.lastModificationField );
    check( "FILE_URI_FIELDNAME", () -> meta.additionalOutputFields.uriField );
    check( "FILE_ROOT_URI_FIELDNAME", () -> meta.additionalOutputFields.rootUriField );

    /////////////////
    check( "ERROR_COUNT_FIELD", () -> meta.errorCountField );
    check( "ERROR_FIELDS_FIELD", () -> meta.errorFieldsField );
    check( "ERROR_TEXT_FIELD", () -> meta.errorTextField );
    check( "ERROR_LINES_SKIPPED", () -> meta.errorLineSkipped );
  }
}
