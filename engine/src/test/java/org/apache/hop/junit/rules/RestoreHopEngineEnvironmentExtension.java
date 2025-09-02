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

package org.apache.hop.junit.rules;

import org.apache.hop.core.HopEnvironment;

/**
 * JUnit 5 Extension that restores all system properties and resets any Hop related environment
 * instances, including full engine initialization. This is the JUnit 5 equivalent of the JUnit 4
 * RestoreHopEngineEnvironment rule.
 */
public class RestoreHopEngineEnvironmentExtension extends RestoreHopEnvironmentExtension {

  @Override
  protected void defaultInit() throws Throwable {
    super.defaultInit();
    HopEnvironment.init();
  }

  @Override
  protected void cleanUp() {
    HopEnvironment.reset();
    super.cleanUp();
  }
}
