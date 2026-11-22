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

package org.apache.hop.hop;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@link Hop#execute(String[])} returns the exit code instead of ending the JVM, so a launcher can
 * run a hop command in its own process. A {@code System.exit} here would end the test JVM.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class HopExecuteTest {

  @Test
  void returnsTheExitCodeOfTheHelpRequest() throws Exception {
    assertEquals(1, Hop.execute(new String[] {"--help"}));
  }
}
