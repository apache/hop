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

package org.apache.hop.metadata.serializer.memory;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

/**
 * Answers from memory and writes each change through to the JSON folder, so a save survives a
 * restart and every stack that shares this provider sees it.
 */
class SharedMetadataSerializer<T extends IHopMetadata> implements IHopMetadataSerializer<T> {

  private final SharedMetadataProvider provider;
  private final IHopMetadataSerializer<T> memory;
  private final IHopMetadataSerializer<T> json;

  SharedMetadataSerializer(
      SharedMetadataProvider provider,
      IHopMetadataSerializer<T> memory,
      IHopMetadataSerializer<T> json) {
    this.provider = provider;
    this.memory = memory;
    this.json = json;
  }

  @Override
  public String getDescription() {
    return memory.getDescription();
  }

  @Override
  public T load(String objectName) throws HopException {
    return memory.load(objectName);
  }

  @Override
  public void save(T object) throws HopException {
    json.save(object);
    memory.save(object);
    object.setMetadataProviderName(provider.getDescription());
  }

  @Override
  public T delete(String name) throws HopException {
    T deleted = null;
    if (json.exists(name)) {
      deleted = json.delete(name);
    }
    if (memory.exists(name)) {
      T fromMemory = memory.delete(name);
      if (deleted == null) {
        deleted = fromMemory;
      }
    }
    if (deleted == null) {
      throw new HopException("Error: Object '" + name + "' doesn't exist");
    }
    return deleted;
  }

  @Override
  public List<String> listObjectNames() throws HopException {
    return memory.listObjectNames();
  }

  @Override
  public String readVirtualPath(String name) throws HopException {
    return memory.readVirtualPath(name);
  }

  @Override
  public boolean exists(String name) throws HopException {
    return memory.exists(name);
  }

  @Override
  public Class<T> getManagedClass() {
    return memory.getManagedClass();
  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return provider;
  }

  @Override
  public List<T> loadAll() throws HopException {
    return memory.loadAll();
  }
}
