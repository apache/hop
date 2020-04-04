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
    check( "FILENAME", new IStringGetter() {
      public String get() {
        return meta.getFileName();
      }
    } );
    check( "PASS_TO_SERVLET", new IBooleanGetter() {
      public boolean get() {
        return meta.isServletOutput();
      }
    } );
    check( "CREATE_PARENT_FOLDER", new IBooleanGetter() {
      public boolean get() {
        return meta.isCreateParentFolder();
      }
    } );
    check( "EXTENSION", new IStringGetter() {
      public String get() {
        return meta.getExtension();
      }
    } );
    check( "SEPARATOR", new IStringGetter() {
      public String get() {
        return meta.getSeparator();
      }
    } );
    check( "ENCLOSURE", new IStringGetter() {
      public String get() {
        return meta.getEnclosure();
      }
    } );
    check( "FORCE_ENCLOSURE", new IBooleanGetter() {
      public boolean get() {
        return meta.isEnclosureForced();
      }
    } );
    check( "DISABLE_ENCLOSURE_FIX", new IBooleanGetter() {
      public boolean get() {
        return meta.isEnclosureFixDisabled();
      }
    } );
    check( "HEADER", new IBooleanGetter() {
      public boolean get() {
        return meta.isHeaderEnabled();
      }
    } );
    check( "FOOTER", new IBooleanGetter() {
      public boolean get() {
        return meta.isFooterEnabled();
      }
    } );
    check( "FORMAT", new IStringGetter() {
      public String get() {
        return meta.getFileFormat();
      }
    } );
    check( "COMPRESSION", new IStringGetter() {
      public String get() {
        return meta.getFileCompression();
      }
    } );
    check( "SPLIT_EVERY", new IIntGetter() {
      public int get() {
        return meta.getSplitEvery();
      }
    } );
    check( "APPEND", new IBooleanGetter() {
      public boolean get() {
        return meta.isFileAppended();
      }
    } );
    check( "INC_TRANSFORMNR_IN_FILENAME", new IBooleanGetter() {
      public boolean get() {
        return meta.isTransformNrInFilename();
      }
    } );
    check( "INC_PARTNR_IN_FILENAME", new IBooleanGetter() {
      public boolean get() {
        return meta.isPartNrInFilename();
      }
    } );
    check( "INC_DATE_IN_FILENAME", new IBooleanGetter() {
      public boolean get() {
        return meta.isDateInFilename();
      }
    } );
    check( "INC_TIME_IN_FILENAME", new IBooleanGetter() {
      public boolean get() {
        return meta.isTimeInFilename();
      }
    } );
    check( "RIGHT_PAD_FIELDS", new IBooleanGetter() {
      public boolean get() {
        return meta.isPadded();
      }
    } );
    check( "FAST_DATA_DUMP", new IBooleanGetter() {
      public boolean get() {
        return meta.isFastDump();
      }
    } );
    check( "ENCODING", new IStringGetter() {
      public String get() {
        return meta.getEncoding();
      }
    } );
    check( "ADD_ENDING_LINE", new IStringGetter() {
      public String get() {
        return meta.getEndedLine();
      }
    } );
    check( "FILENAME_IN_FIELD", new IBooleanGetter() {
      public boolean get() {
        return meta.isFileNameInField();
      }
    } );
    check( "FILENAME_FIELD", new IStringGetter() {
      public String get() {
        return meta.getFileNameField();
      }
    } );
    check( "NEW_LINE", new IStringGetter() {
      public String get() {
        return meta.getNewline();
      }
    } );
    check( "ADD_TO_RESULT", new IBooleanGetter() {
      public boolean get() {
        return meta.isAddToResultFiles();
      }
    } );
    check( "DO_NOT_CREATE_FILE_AT_STARTUP", new IBooleanGetter() {
      public boolean get() {
        return meta.isDoNotOpenNewFileInit();
      }
    } );
    check( "SPECIFY_DATE_FORMAT", new IBooleanGetter() {
      public boolean get() {
        return meta.isSpecifyingFormat();
      }
    } );
    check( "DATE_FORMAT", new IStringGetter() {
      public String get() {
        return meta.getDateTimeFormat();
      }
    } );

    /////////////////////////////
    check( "OUTPUT_FIELDNAME", new IStringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getName();
      }
    } );

    // TODO check field type plugins
    skipPropertyTest( "OUTPUT_TYPE" );

    check( "OUTPUT_FORMAT", new IStringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getFormat();
      }
    } );
    check( "OUTPUT_LENGTH", new IIntGetter() {
      public int get() {
        return meta.getOutputFields()[ 0 ].getLength();
      }
    } );
    check( "OUTPUT_PRECISION", new IIntGetter() {
      public int get() {
        return meta.getOutputFields()[ 0 ].getPrecision();
      }
    } );
    check( "OUTPUT_CURRENCY", new IStringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getCurrencySymbol();
      }
    } );
    check( "OUTPUT_DECIMAL", new IStringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getDecimalSymbol();
      }
    } );
    check( "OUTPUT_GROUP", new IStringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getGroupingSymbol();
      }
    } );
    check( "OUTPUT_NULL", new IStringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getNullString();
      }
    } );
    check( "RUN_AS_COMMAND", new IBooleanGetter() {
      public boolean get() {
        return meta.isFileAsCommand();
      }
    } );

    IValueMeta mftt = new ValueMetaString( "f" );
    injector.setProperty( meta, "OUTPUT_TRIM", setValue( mftt, "left" ), "f" );
    assertEquals( 1, meta.getOutputFields()[ 0 ].getTrimType() );
    injector.setProperty( meta, "OUTPUT_TRIM", setValue( mftt, "right" ), "f" );
    assertEquals( 2, meta.getOutputFields()[ 0 ].getTrimType() );
    skipPropertyTest( "OUTPUT_TRIM" );
  }
}
