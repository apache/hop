/*! ******************************************************************************
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

package org.apache.hop.trans.steps.xbaseinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransTestFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class XBaseInputIntIT {

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  List<RowMetaAndData> getEmptyRowMetaAndData() {
    return new ArrayList<RowMetaAndData>();
  }

  /*
   * Timeout is needed, as the transformation may never stop if PDI-8846 regresses
   */
  @Test( timeout = 10000 )
  public void testFilenameFromFieldNoFiles() throws HopException {
    String stepName = "XBase Input";
    XBaseInputMeta meta = new XBaseInputMeta();
    meta.setAcceptingFilenames( true );
    meta.setAcceptingField( "filename" );
    meta.setAcceptingStepName( TransTestFactory.INJECTOR_STEPNAME );

    TransMeta transMeta = TransTestFactory.generateTestTransformation( null, meta, stepName );
    List<RowMetaAndData> inputList = getEmptyRowMetaAndData();
    List<RowMetaAndData> ret =
      TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
        TransTestFactory.DUMMY_STEPNAME, inputList );

    assertNotNull( ret );
    assertEquals( 0, ret.size() );
  }
}
