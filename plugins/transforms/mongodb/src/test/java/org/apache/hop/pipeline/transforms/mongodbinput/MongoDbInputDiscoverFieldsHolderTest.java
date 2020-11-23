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

package org.apache.hop.pipeline.transforms.mongodbinput;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class MongoDbInputDiscoverFieldsHolderTest {

  @Mock MongoDbInputDiscoverFields discoverFields;
  @Mock MongoDbInputDiscoverFields discoverFields2;
  @Mock MongoDbInputDiscoverFields discoverFields3b;
  @Mock MongoDbInputDiscoverFields discoverFields3;
  MongoDbInputDiscoverFieldsHolder holder = new MongoDbInputDiscoverFieldsHolder();

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testImplAddedRemoved() throws Exception {
    holder.implAdded(discoverFields, ImmutableMap.of("service.ranking", 1));
    holder.implAdded(discoverFields2, Collections.emptyMap());
    holder.implAdded(discoverFields3, ImmutableMap.of("service.ranking", 5));
    holder.implAdded(
        discoverFields3b, ImmutableMap.of("service.ranking", 5)); // second impl at same rank

    assertThat(holder.getMongoDbInputDiscoverFields(), equalTo(discoverFields3));
    holder.implRemoved(discoverFields3, ImmutableMap.of("service.ranking", 5));
    assertThat(holder.getMongoDbInputDiscoverFields(), equalTo(discoverFields3b));
    holder.implRemoved(discoverFields3b, ImmutableMap.of("service.ranking", 5));
    assertThat(holder.getMongoDbInputDiscoverFields(), equalTo(discoverFields));
    holder.implRemoved(discoverFields, ImmutableMap.of("service.ranking", 1));
    assertThat(holder.getMongoDbInputDiscoverFields(), equalTo(discoverFields2));
  }
}
