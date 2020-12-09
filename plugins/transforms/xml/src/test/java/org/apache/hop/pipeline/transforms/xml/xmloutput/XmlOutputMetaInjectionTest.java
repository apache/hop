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
package org.apache.hop.pipeline.transforms.xml.xmloutput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class XmlOutputMetaInjectionTest extends BaseMetadataInjectionTest<XmlOutputMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  @Before
  public void setup() {

    try{
      setup( new XmlOutputMeta() );
    }catch(Exception e){

    }
  }

  @Test
  public void test() throws Exception {
    check( "FILENAME", () -> meta.getFileName() );
    check( "EXTENSION", () -> meta.getExtension() );
    check( "PASS_TO_SERVLET", () -> meta.isServletOutput() );
    check( "SPLIT_EVERY", () -> meta.getSplitEvery() );
    check( "INC_TRANSFORMNR_IN_FILENAME", () -> meta.isTransformNrInFilename() );
    check( "INC_DATE_IN_FILENAME", () -> meta.isDateInFilename() );
    check( "INC_TIME_IN_FILENAME", () -> meta.isTimeInFilename() );
    check( "ZIPPED", () -> meta.isZipped() );
    check( "ENCODING", () -> meta.getEncoding() );
    check( "NAMESPACE", () -> meta.getNameSpace() );
    check( "MAIN_ELEMENT", () -> meta.getMainElement() );
    check( "REPEAT_ELEMENT", () -> meta.getRepeatElement() );
    check( "ADD_TO_RESULT", () -> meta.isAddToResultFiles() );
    check( "DO_NOT_CREATE_FILE_AT_STARTUP", () -> meta.isDoNotOpenNewFileInit() );
    check( "OMIT_NULL_VALUES", () -> meta.isOmitNullValues() );
    check( "SPEFICY_FORMAT", () -> meta.isSpecifyFormat() );
    check( "DATE_FORMAT", () -> meta.getDateTimeFormat() );
    check( "OUTPUT_FIELDNAME", () -> meta.getOutputFields()[0].getFieldName() );
    check( "OUTPUT_ELEMENTNAME", () -> meta.getOutputFields()[0].getElementName() );
    check( "OUTPUT_FORMAT", () -> meta.getOutputFields()[0].getFormat() );
    check( "OUTPUT_LENGTH", () -> meta.getOutputFields()[0].getLength() );
    check( "OUTPUT_PRECISION", () -> meta.getOutputFields()[0].getPrecision() );
    check( "OUTPUT_CURRENCY", () -> meta.getOutputFields()[0].getCurrencySymbol() );
    check( "OUTPUT_DECIMAL", () -> meta.getOutputFields()[0].getDecimalSymbol() );
    check( "OUTPUT_GROUP", () -> meta.getOutputFields()[0].getGroupingSymbol() );
    check( "OUTPUT_NULL", () -> meta.getOutputFields()[0].getNullString() );
    check( "OUTPUT_CONTENT_TYPE", () -> meta.getOutputFields()[0].getContentType(), XmlField.ContentType.class );

    // TODO check field type plugins
    skipPropertyTest( "OUTPUT_TYPE" );
  }
}
