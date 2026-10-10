/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.commons.vfs2;

/**
 * The read path of a provider only: listing, content, large sequential reads and random access. For
 * object stores whose write, rename and naming semantics differ from a file system but whose reads
 * must behave like one. The fixture tree is expected under the base folder; see {@link
 * ProviderTestFixtures#copyTo(FileObject)}.
 */
public class ProviderReadTestSuite extends AbstractProviderTestSuite {

  public ProviderReadTestSuite(final ProviderTestConfig providerConfig) {
    super(providerConfig, "", false);
  }

  @Override
  protected void addBaseTests() throws Exception {
    addTests(ProviderReadTests.class);
    addTests(ContentTests.class);
    addTests(ProviderLargeReadTests.class);
    addTests(ProviderRandomReadTests.class);
    addTests(ProviderRangeReadTests.class);
  }
}
