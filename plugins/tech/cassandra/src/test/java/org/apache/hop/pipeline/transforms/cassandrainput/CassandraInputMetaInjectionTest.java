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
 *
 */
package org.apache.hop.pipeline.transforms.cassandrainput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.junit.Before;
import org.junit.Test;

public class CassandraInputMetaInjectionTest extends BaseMetadataInjectionTest<CassandraInputMeta> {

  @Before
  public void setup() throws Exception {
    setup(new CassandraInputMeta());
  }

  @Test
  public void test() throws Exception {
    check("CONNECTION", () -> meta.getConnectionName());
    check("CQL_QUERY", () -> meta.getCqlSelectQuery());
    check("EXECUTE_FOR_EACH_ROW", () -> meta.isExecuteForEachIncomingRow());
    check("TRANSPORT_MAX_LENGTH", () -> meta.getMaxLength());
  }
}
