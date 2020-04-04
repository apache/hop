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

package org.apache.hop.job.entry;

import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.special.JobEntrySpecial;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class JobEntryCopyTest {

  private static final String ATTRIBUTE_GROUP = "aGroupName";
  private static final String ATTRIBUTE_KEY = "someKey";
  private static final String ATTRIBUTE_VALUE = "aValue";
  private JobEntryCopy originJobEntry;
  private JobEntryCopy copyJobEntry;
  private IJobEntry originEntry;

  @Before
  public void setUp() throws Exception {
    originJobEntry = new JobEntryCopy();
    copyJobEntry = new JobEntryCopy();

    originEntry = new JobEntrySpecial( "EntrySpecial", false, false );
    originEntry.setChanged( false );

    originJobEntry.setEntry( originEntry );
    originJobEntry.setAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY, ATTRIBUTE_VALUE );
  }

  @Test
  public void testReplaceMetaCloneEntryOfOrigin() throws Exception {

    copyJobEntry.replaceMeta( originJobEntry );
    assertNotSame( "Entry of origin and copy JobEntry should be different objects: ", copyJobEntry.getEntry(),
      originJobEntry.getEntry() );
  }

  @Test
  public void testReplaceMetaDoesNotChangeEntryOfOrigin() throws Exception {

    copyJobEntry.replaceMeta( originJobEntry );
    assertEquals( "hasChanged in Entry of origin JobEntry should not be changed. ", false, originJobEntry.getEntry()
      .hasChanged() );
  }

  @Test
  public void testReplaceMetaChangesEntryOfCopy() throws Exception {

    copyJobEntry.replaceMeta( originJobEntry );
    assertEquals( "hasChanged in Entry of copy JobEntry should be changed. ", true, copyJobEntry.getEntry()
      .hasChanged() );
  }

  @Test
  public void testSetParentMeta() throws Exception {
    JobMeta meta = Mockito.mock( JobMeta.class );
    originJobEntry.setParentJobMeta( meta );
    assertEquals( meta, originEntry.getParentJobMeta() );
  }

  @Test
  public void testCloneClonesAttributesMap() throws Exception {

    JobEntryCopy clonedJobEntry = (JobEntryCopy) originJobEntry.clone();
    assertNotNull( clonedJobEntry.getAttributesMap() );
    assertEquals( originJobEntry.getAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY ),
      clonedJobEntry.getAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY ) );
  }

  @Test
  public void testDeepCloneClonesAttributesMap() throws Exception {

    JobEntryCopy deepClonedJobEntry = (JobEntryCopy) originJobEntry.clone_deep();
    assertNotNull( deepClonedJobEntry.getAttributesMap() );
    assertEquals( originJobEntry.getAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY ),
      deepClonedJobEntry.getAttribute( ATTRIBUTE_GROUP, ATTRIBUTE_KEY ) );
  }

}
