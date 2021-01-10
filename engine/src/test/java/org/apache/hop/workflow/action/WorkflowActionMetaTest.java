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

package org.apache.hop.workflow.action;

import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class WorkflowActionMetaTest {

  private static final String ATTRIBUTE_GROUP = "aGroupName";
  private static final String ATTRIBUTE_KEY = "someKey";
  private static final String ATTRIBUTE_VALUE = "aValue";
  private ActionMeta originActionMeta;
  private ActionMeta copyActionMeta;
  private IAction originAction;

  @Before
  public void setUp() throws Exception {
    originActionMeta = new ActionMeta();
    copyActionMeta = new ActionMeta();

    originAction = new ActionStart( "EntrySpecial" );
    originAction.setChanged( false );

    originActionMeta.setAction( originAction );
    originActionMeta.setAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY, ATTRIBUTE_VALUE );
  }

  @Test
  public void testReplaceMetaCloneEntryOfOrigin() throws Exception {

    copyActionMeta.replaceMeta( originActionMeta );
    assertNotSame( "Entry of origin and copy Action should be different objects: ", copyActionMeta.getAction(),
      originActionMeta.getAction() );
  }

  @Test
  public void testReplaceMetaDoesNotChangeEntryOfOrigin() throws Exception {

    copyActionMeta.replaceMeta( originActionMeta );
    assertEquals( "hasChanged in Entry of origin Action should not be changed. ", false, originActionMeta.getAction()
      .hasChanged() );
  }

  @Test
  public void testReplaceMetaChangesEntryOfCopy() throws Exception {

    copyActionMeta.replaceMeta( originActionMeta );
    assertEquals( "hasChanged in Entry of copy Action should be changed. ", true, copyActionMeta.getAction()
      .hasChanged() );
  }

  @Test
  public void testSetParentMeta() throws Exception {
    WorkflowMeta meta = Mockito.mock( WorkflowMeta.class );
    originActionMeta.setParentWorkflowMeta( meta );
    assertEquals( meta, originAction.getParentWorkflowMeta() );
  }

  @Test
  public void testCloneClonesAttributesMap() throws Exception {

    ActionMeta clonedActionMeta = (ActionMeta) originActionMeta.clone();
    assertNotNull( clonedActionMeta.getAttributesMap() );
    assertEquals( originActionMeta.getAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY ),
      clonedActionMeta.getAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY ) );
  }

  @Test
  public void testDeepCloneClonesAttributesMap() throws Exception {

    ActionMeta deepClonedActionMeta = (ActionMeta) originActionMeta.cloneDeep();
    assertNotNull( deepClonedActionMeta.getAttributesMap() );
    assertEquals( originActionMeta.getAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY ),
      deepClonedActionMeta.getAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY ) );
  }

}
