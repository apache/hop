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

package org.apache.hop.metadata.serializer.multi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;

/** A provider that cannot be read does not hide objects in an earlier provider. */
class MultiMetadataUnavailableProviderTest {

  @Test
  void loadReturnsTheSharedObjectWhenALaterProviderThrows() throws Exception {
    Variables variables = new Variables();
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();
    MemoryMetadataProvider shared = new MemoryMetadataProvider(encoder, variables);
    shared.setDescription("Shared metadata");
    shared.getSerializer(MetadataType1.class).save(new MetadataType1("git-project", "shared", "1"));

    MultiMetadataProvider multi =
        new MultiMetadataProvider(
            encoder, List.of(shared, new ThrowingProvider(encoder, variables)), variables);

    MetadataType1 loaded = multi.getSerializer(MetadataType1.class).load("git-project");
    assertEquals("shared", loaded.getDescription());
    assertTrue(multi.getSerializer(MetadataType1.class).exists("git-project"));
    assertEquals(
        List.of("git-project"), multi.getSerializer(MetadataType1.class).listObjectNames());
  }

  private static final class ThrowingProvider extends MemoryMetadataProvider {
    private ThrowingProvider(HopTwoWayPasswordEncoder encoder, Variables variables) {
      super(encoder, variables);
      setDescription("unavailable project");
    }

    @Override
    public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer(Class<T> managedClass)
        throws HopException {
      throw new HopException("project folder is not readable yet");
    }
  }
}
