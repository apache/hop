/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.xml.xmlinputstream;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

public class XmlInputStreamInputContentParsingTest extends BaseXmlInputStreamParsingTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  @Test
  public void testDefaultOptions() throws Exception {
    init( "default.xml" );

    process();

    check( new Object[][] { { "START_DOCUMENT", 0L, null, 0L, "", null, null, null },
      { "START_ELEMENT", 1L, 0L, 1L, "/xml", "", "xml", null },
      { "START_ELEMENT", 2L, 1L, 2L, "/xml/tag", "/xml", "tag", null },
      { "ATTRIBUTE", 2L, 1L, 2L, "/xml/tag", "/xml", "a", "1" },
      { "CHARACTERS", 2L, 1L, 2L, "/xml/tag", "/xml", "tag", "zz" },
      { "END_ELEMENT", 2L, 1L, 2L, "/xml/tag", "/xml", "tag", null },
      { "END_ELEMENT", 1L, 0L, 1L, "/xml", "", "xml", null }, { "END_DOCUMENT", 0L, null, 0L, "", null, null, null } } );
  }
}
