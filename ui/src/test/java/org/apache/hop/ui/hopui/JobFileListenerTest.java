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

package org.apache.hop.ui.hopui;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class JobFileListenerTest {

  JobFileListener jobFileListener;

  @Before
  public void setUp() {
    jobFileListener = new JobFileListener();
  }

  @Test
  public void testAccepts() throws Exception {
    assertFalse( jobFileListener.accepts( null ) );
    assertFalse( jobFileListener.accepts( "NoDot" ) );
    assertTrue( jobFileListener.accepts( "Job.kjb" ) );
    assertTrue( jobFileListener.accepts( ".kjb" ) );
  }

  @Test
  public void testAcceptsXml() throws Exception {
    assertFalse( jobFileListener.acceptsXml( null ) );
    assertFalse( jobFileListener.acceptsXml( "" ) );
    assertFalse( jobFileListener.acceptsXml( "Job" ) );
    assertTrue( jobFileListener.acceptsXml( "job" ) );
  }

  @Test
  public void testGetFileTypeDisplayNames() throws Exception {
    String[] names = jobFileListener.getFileTypeDisplayNames( null );
    assertNotNull( names );
    assertEquals( 2, names.length );
    assertEquals( "Jobs", names[ 0 ] );
    assertEquals( "XML", names[ 1 ] );
  }

  @Test
  public void testGetRootNodeName() throws Exception {
    assertEquals( "job", jobFileListener.getRootNodeName() );
  }

  @Test
  public void testGetSupportedExtensions() throws Exception {
    String[] extensions = jobFileListener.getSupportedExtensions();
    assertNotNull( extensions );
    assertEquals( 2, extensions.length );
    assertEquals( "kjb", extensions[ 0 ] );
    assertEquals( "xml", extensions[ 1 ] );
  }
}
