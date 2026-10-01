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
package org.apache.hop.lint;

import org.apache.hop.core.HopClientEnvironment;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Auto-detected JUnit 5 extension that initialises the Hop client environment once per test JVM.
 *
 * <p>Production code logs through {@code LogChannel.GENERAL}, which throws {@code Central Log Store
 * is not initialized} until {@link HopClientEnvironment#init()} has run. Registering this globally
 * (see {@code META-INF/services/org.junit.jupiter.api.extension.Extension}) means every test class
 * gets a usable environment without repeating boilerplate.
 */
public class HopLintTestEnvironment implements BeforeAllCallback {

  private static volatile boolean initialised = false;

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    init();
  }

  public static synchronized void init() throws Exception {
    if (!initialised) {
      HopClientEnvironment.init();
      initialised = true;
    }
  }
}
