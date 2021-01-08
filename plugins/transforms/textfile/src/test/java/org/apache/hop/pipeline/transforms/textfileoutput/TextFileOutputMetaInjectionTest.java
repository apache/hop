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

package org.apache.hop.pipeline.transforms.textfileoutput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TextFileOutputMetaInjectionTest extends BaseMetadataInjectionTest<TextFileOutputMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new TextFileOutputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FILENAME", () -> meta.getFileName() );
    check( "PASS_TO_SERVLET", () -> meta.isServletOutput() );
    check( "CREATE_PARENT_FOLDER", () -> meta.isCreateParentFolder() );
    check( "EXTENSION", () -> meta.getExtension() );
    check( "SEPARATOR", () -> meta.getSeparator() );
    check( "ENCLOSURE", () -> meta.getEnclosure() );
    check( "FORCE_ENCLOSURE", () -> meta.isEnclosureForced() );
    check( "DISABLE_ENCLOSURE_FIX", () -> meta.isEnclosureFixDisabled() );
    check( "HEADER", () -> meta.isHeaderEnabled() );
    check( "FOOTER", () -> meta.isFooterEnabled() );
    check( "FORMAT", () -> meta.getFileFormat() );
    check( "COMPRESSION", () -> meta.getFileCompression() );
    check( "SPLIT_EVERY", () -> meta.getSplitEvery() );
    check( "APPEND", () -> meta.isFileAppended() );
    check( "INC_TRANSFORMNR_IN_FILENAME", () -> meta.isTransformNrInFilename() );
    check( "INC_PARTNR_IN_FILENAME", () -> meta.isPartNrInFilename() );
    check( "INC_DATE_IN_FILENAME", () -> meta.isDateInFilename() );
    check( "INC_TIME_IN_FILENAME", () -> meta.isTimeInFilename() );
    check( "RIGHT_PAD_FIELDS", () -> meta.isPadded() );
    check( "FAST_DATA_DUMP", () -> meta.isFastDump() );
    check( "ENCODING", () -> meta.getEncoding() );
    check( "ADD_ENDING_LINE", () -> meta.getEndedLine() );
    check( "FILENAME_IN_FIELD", () -> meta.isFileNameInField() );
    check( "FILENAME_FIELD", () -> meta.getFileNameField() );
    check( "NEW_LINE", () -> meta.getNewline() );
    check( "ADD_TO_RESULT", () -> meta.isAddToResultFiles() );
    check( "DO_NOT_CREATE_FILE_AT_STARTUP", () -> meta.isDoNotOpenNewFileInit() );
    check( "SPECIFY_DATE_FORMAT", () -> meta.isSpecifyingFormat() );
    check( "DATE_FORMAT", () -> meta.getDateTimeFormat() );

    /////////////////////////////
    check( "OUTPUT_FIELDNAME", () -> meta.getOutputFields()[ 0 ].getName() );

    // TODO check field type plugins
    skipPropertyTest( "OUTPUT_TYPE" );

    check( "OUTPUT_FORMAT", () -> meta.getOutputFields()[ 0 ].getFormat() );
    check( "OUTPUT_LENGTH", () -> meta.getOutputFields()[ 0 ].getLength() );
    check( "OUTPUT_PRECISION", () -> meta.getOutputFields()[ 0 ].getPrecision() );
    check( "OUTPUT_CURRENCY", () -> meta.getOutputFields()[ 0 ].getCurrencySymbol() );
    check( "OUTPUT_DECIMAL", () -> meta.getOutputFields()[ 0 ].getDecimalSymbol() );
    check( "OUTPUT_GROUP", () -> meta.getOutputFields()[ 0 ].getGroupingSymbol() );
    check( "OUTPUT_NULL", () -> meta.getOutputFields()[ 0 ].getNullString() );
    check( "RUN_AS_COMMAND", () -> meta.isFileAsCommand() );

    IValueMeta mftt = new ValueMetaString( "f" );
    injector.setProperty( meta, "OUTPUT_TRIM", setValue( mftt, "left" ), "f" );
    assertEquals( 1, meta.getOutputFields()[ 0 ].getTrimType() );
    injector.setProperty( meta, "OUTPUT_TRIM", setValue( mftt, "right" ), "f" );
    assertEquals( 2, meta.getOutputFields()[ 0 ].getTrimType() );
    skipPropertyTest( "OUTPUT_TRIM" );
  }
}
