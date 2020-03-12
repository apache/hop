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
package org.apache.hop.job.entries.mail;

import junit.framework.Assert;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class JobEntryTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setupBeforeClass() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  public void testJobEntrymailPasswordFixed() {
    JobEntryMail jem = new JobEntryMail();
    Assert.assertEquals( jem.getPassword( "asdf" ), "asdf" );
  }

  @Test
  public void testJobEntrymailPasswordEcr() {
    JobEntryMail jem = new JobEntryMail();
    Assert.assertEquals( jem.getPassword( "Encrypted 2be98afc86aa7f2e4cb79ce10df81abdc" ), "asdf" );
  }

  @Test
  public void testJobEntrymailPasswordVar() {
    JobEntryMail jem = new JobEntryMail();
    jem.setVariable( "my_pass", "asdf" );
    Assert.assertEquals( jem.getPassword( "${my_pass}" ), "asdf" );
  }

  @Test
  public void testJobEntrymailPasswordEncrVar() {
    JobEntryMail jem = new JobEntryMail();
    jem.setVariable( "my_pass", "Encrypted 2be98afc86aa7f2e4cb79ce10df81abdc" );
    Assert.assertEquals( jem.getPassword( "${my_pass}" ), "asdf" );
  }
}
