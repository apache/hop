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
package org.apache.hop.ui.hopui;

import org.junit.Before;
import org.junit.Test;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.jobexecutor.JobExecutorMeta;
import org.apache.hop.trans.steps.transexecutor.TransExecutorMeta;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class TransFileListenerTest {

  TransFileListener transFileListener;

  @Before
  public void setUp() {
    transFileListener = new TransFileListener();
  }

  @Test
  public void testAccepts() throws Exception {
    assertFalse( transFileListener.accepts( null ) );
    assertFalse( transFileListener.accepts( "NoDot" ) );
    assertTrue( transFileListener.accepts( "Trans.ktr" ) );
    assertTrue( transFileListener.accepts( ".ktr" ) );
  }

  @Test
  public void testAcceptsXml() throws Exception {
    assertFalse( transFileListener.acceptsXml( null ) );
    assertFalse( transFileListener.acceptsXml( "" ) );
    assertFalse( transFileListener.acceptsXml( "Transformation" ) );
    assertTrue( transFileListener.acceptsXml( "transformation" ) );
  }

  @Test
  public void testGetFileTypeDisplayNames() throws Exception {
    String[] names = transFileListener.getFileTypeDisplayNames( null );
    assertNotNull( names );
    assertEquals( 2, names.length );
    assertEquals( "Transformations", names[0] );
    assertEquals( "XML", names[1] );
  }

  @Test
  public void testGetRootNodeName() throws Exception {
    assertEquals( "transformation", transFileListener.getRootNodeName() );
  }

  @Test
  public void testGetSupportedExtensions() throws Exception {
    String[] extensions = transFileListener.getSupportedExtensions();
    assertNotNull( extensions );
    assertEquals( 2, extensions.length );
    assertEquals( "ktr", extensions[0] );
    assertEquals( "xml", extensions[1] );
  }


}
