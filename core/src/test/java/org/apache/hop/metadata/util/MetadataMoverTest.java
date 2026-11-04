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

package org.apache.hop.metadata.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MetadataType1;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.junit.jupiter.api.Test;

class MetadataMoverTest {

  @Test
  void newObjectIsRetargetedWithoutWriting() throws Exception {
    MultiMetadataProvider multi = stack();
    MetadataType1 metadata = new MetadataType1("new", "d", "v");
    MetadataMoveStatus status =
        MetadataMover.move(multi, metadata, multi.getProviders().get(0).getDescription(), false);
    assertEquals(MetadataMoveStatus.RETARGETED, status);
    assertEquals(multi.getProviders().get(0).getDescription(), metadata.getMetadataProviderName());
    assertFalse(multi.getProviders().get(0).getSerializer(MetadataType1.class).exists("new"));
  }

  @Test
  void moveDeletesTheSourceAndKeepsTheTarget() throws Exception {
    MultiMetadataProvider multi = stack();
    IHopMetadataProvider source = multi.getProviders().get(1);
    IHopMetadataProvider target = multi.getProviders().get(0);
    MetadataType1 metadata = new MetadataType1("db", "d", "v");
    source.getSerializer(MetadataType1.class).save(metadata);

    MetadataMoveStatus status = MetadataMover.move(multi, metadata, target.getDescription(), true);

    assertEquals(MetadataMoveStatus.MOVED, status);
    assertTrue(target.getSerializer(MetadataType1.class).exists("db"));
    assertFalse(source.getSerializer(MetadataType1.class).exists("db"));
    assertEquals(
        target.getDescription(),
        multi.getSerializer(MetadataType1.class).load("db").getMetadataProviderName());
  }

  @Test
  void collisionLeavesBothCopies() throws Exception {
    MultiMetadataProvider multi = stack();
    IHopMetadataProvider source = multi.getProviders().get(1);
    IHopMetadataProvider target = multi.getProviders().get(0);
    source.getSerializer(MetadataType1.class).save(new MetadataType1("db", "source", "1"));
    target.getSerializer(MetadataType1.class).save(new MetadataType1("db", "target", "2"));
    MetadataType1 metadata = source.getSerializer(MetadataType1.class).load("db");

    MetadataMoveStatus status = MetadataMover.move(multi, metadata, target.getDescription(), true);

    assertEquals(MetadataMoveStatus.COLLISION, status);
    assertEquals("source", source.getSerializer(MetadataType1.class).load("db").getDescription());
    assertEquals("target", target.getSerializer(MetadataType1.class).load("db").getDescription());
  }

  private static MultiMetadataProvider stack() {
    Variables variables = new Variables();
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();
    MemoryMetadataProvider first = new MemoryMetadataProvider(encoder, variables);
    first.setDescription("Provider1");
    MemoryMetadataProvider second = new MemoryMetadataProvider(encoder, variables);
    second.setDescription("Provider2");
    return new MultiMetadataProvider(encoder, List.of(first, second), variables);
  }
}
