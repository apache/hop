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
package org.apache.hop.mail.workflow.actions.mail;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

class WorkflowEntryTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setupBeforeClass() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  void testJobEntrymailPasswordFixed() {
    ActionMail jem = new ActionMail();
    assertEquals("asdf", jem.getPassword("asdf"));
  }

  @Test
  void testJobEntrymailPasswordEcr() {
    ActionMail jem = new ActionMail();
    assertEquals("asdf", jem.getPassword("Encrypted 2be98afc86aa7f2e4cb79ce10df81abdc"));
  }

  @Test
  void testJobEntrymailPasswordVar() {
    ActionMail jem = new ActionMail();
    jem.setVariable("my_pass", "asdf");
    assertEquals("asdf", jem.getPassword("${my_pass}"));
  }

  @Test
  void testJobEntrymailPasswordEncrVar() {
    ActionMail jem = new ActionMail();
    jem.setVariable("my_pass", "Encrypted 2be98afc86aa7f2e4cb79ce10df81abdc");
    assertEquals("asdf", jem.getPassword("${my_pass}"));
  }
}
