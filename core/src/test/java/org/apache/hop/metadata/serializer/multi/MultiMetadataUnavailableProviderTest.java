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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
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

  /**
   * A child file that exists but cannot be parsed (git conflict markers) must fail the load. The
   * parent object's copy of the same name is a different connection.
   */
  @Test
  void loadThrowsWhenTheChildObjectExistsButCannotBeRead() throws Exception {
    Variables variables = new Variables();
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();
    MemoryMetadataProvider parent = new MemoryMetadataProvider(encoder, variables);
    parent.setDescription("parent project");
    parent.getSerializer(MetadataType1.class).save(new MetadataType1("dwh", "parent", "1"));
    parent.getSerializer(MetadataType1.class).save(new MetadataType1("warehouse", "parent", "2"));

    BrokenLoadProvider child = new BrokenLoadProvider(encoder, variables);
    child.getSerializer(MetadataType1.class).save(new MetadataType1("dwh", "child", "broken"));

    MultiMetadataProvider multi =
        new MultiMetadataProvider(encoder, List.of(parent, child), variables);
    IHopMetadataSerializer<MetadataType1> serializer = multi.getSerializer(MetadataType1.class);

    HopException loadFailure = assertThrows(HopException.class, () -> serializer.load("dwh"));
    assertTrue(loadFailure.getMessage().contains("git conflict markers"));
    HopException readFailure =
        assertThrows(HopException.class, () -> serializer.readVirtualPath("dwh"));
    assertTrue(readFailure.getMessage().contains("git conflict markers"));

    assertTrue(serializer.exists("dwh"));
    MetadataType1 other = serializer.load("warehouse");
    assertEquals("parent", other.getDescription());
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

  /** The file is present, so {@code exists()} is true, but the contents cannot be parsed. */
  private static final class BrokenLoadProvider extends MemoryMetadataProvider {
    private BrokenLoadProvider(HopTwoWayPasswordEncoder encoder, Variables variables) {
      super(encoder, variables);
      setDescription("child project");
    }

    @Override
    public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer(Class<T> managedClass)
        throws HopException {
      return new BrokenLoadSerializer<>(super.getSerializer(managedClass));
    }
  }

  private static final class BrokenLoadSerializer<T extends IHopMetadata>
      implements IHopMetadataSerializer<T> {
    private final IHopMetadataSerializer<T> delegate;

    private BrokenLoadSerializer(IHopMetadataSerializer<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public String getDescription() {
      return delegate.getDescription();
    }

    @Override
    public T load(String objectName) throws HopException {
      if (delegate.exists(objectName)) {
        throw new HopException("git conflict markers in '" + objectName + ".json'");
      }
      return delegate.load(objectName);
    }

    @Override
    public void save(T object) throws HopException {
      delegate.save(object);
    }

    @Override
    public T delete(String name) throws HopException {
      return delegate.delete(name);
    }

    @Override
    public List<String> listObjectNames() throws HopException {
      return delegate.listObjectNames();
    }

    @Override
    public String readVirtualPath(String name) throws HopException {
      if (delegate.exists(name)) {
        throw new HopException("git conflict markers in '" + name + ".json'");
      }
      return delegate.readVirtualPath(name);
    }

    @Override
    public boolean exists(String name) throws HopException {
      return delegate.exists(name);
    }

    @Override
    public Class<T> getManagedClass() {
      return delegate.getManagedClass();
    }

    @Override
    public IHopMetadataProvider getMetadataProvider() {
      return delegate.getMetadataProvider();
    }

    @Override
    public List<T> loadAll() throws HopException {
      return delegate.loadAll();
    }
  }
}
