/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2;

/**
 * JUnit 5 version of {@link ProviderTestSuite}.
 *
 * <p>This class provides the standard set of provider tests using JUnit 5's {@code @TestFactory}
 * pattern.
 */
public class ProviderTestSuiteJunit5 extends AbstractProviderTestSuite {

  public ProviderTestSuiteJunit5(final ProviderTestConfig providerConfig) {
    this(providerConfig, "", false);
  }

  public ProviderTestSuiteJunit5(
      final ProviderTestConfig providerConfig, final String prefix, final boolean addEmptyDir) {
    super(providerConfig, prefix, addEmptyDir);
  }

  /** Adds base tests - excludes the nested test cases. */
  @Override
  protected void addBaseTests() throws Exception {
    addTests(UrlTests.class);
    addTests(ProviderCacheStrategyTests.class);
    addTests(UriTests.class);
    addTests(NamingTests.class);
    addTests(ContentTests.class);
    addTests(ProviderReadTests.class);
    addTests(ProviderWriteTests.class);
    addTests(ProviderWriteAppendTests.class);
    addTests(ProviderRandomReadTests.class);
    addTests(ProviderRandomReadWriteTests.class);
    addTests(ProviderRandomSetLengthTests.class);
    addTests(ProviderRenameTests.class);
    addTests(ProviderDeleteTests.class);
    addTests(LastModifiedTests.class);
    addTests(UrlStructureTests.class);
    // VfsClassLoaderTests is left out: it exercises the VFS class loader rather than the
    // provider, and needs a binary jar fixture Hop has no reason to carry.
  }
}
