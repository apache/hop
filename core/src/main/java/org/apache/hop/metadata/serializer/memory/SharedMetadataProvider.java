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

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;

/**
 * The Hop configuration's metadata, loaded once from a local folder and then shared by every
 * metadata stack in the process.
 */
public class SharedMetadataProvider extends MemoryMetadataProvider {

  public static final String DESCRIPTION = "Shared metadata";

  @Getter private final String folder;
  @Getter private final String configuredFolder;

  private final JsonMetadataProvider store;
  private final Map<String, IHopMetadataSerializer<?>> serializers = new HashMap<>();

  public SharedMetadataProvider(IVariables variables, String folder, String configuredFolder)
      throws HopException {
    super(encoder(), variables);
    setDescription(DESCRIPTION);
    this.folder = folder;
    this.configuredFolder = configuredFolder;
    this.store = new JsonMetadataProvider(encoder(), folder, variables);
    loadFromFolder();
  }

  private static ITwoWayPasswordEncoder encoder() {
    ITwoWayPasswordEncoder encoder = Encr.getEncoder();
    if (encoder == null) {
      encoder = new HopTwoWayPasswordEncoder();
    }
    return encoder;
  }

  private void loadFromFolder() throws HopException {
    for (Class<IHopMetadata> managedClass : this.<IHopMetadata>getMetadataClasses()) {
      IHopMetadataSerializer<IHopMetadata> json = store.getSerializer(managedClass);
      IHopMetadataSerializer<IHopMetadata> memory = super.getSerializer(managedClass);
      for (IHopMetadata object : json.loadAll()) {
        if (object != null) {
          memory.save(object);
        }
      }
    }
  }

  @Override
  public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer(Class<T> managedClass)
      throws HopException {
    IHopMetadataSerializer<?> existing = serializers.get(managedClass.getName());
    if (existing != null) {
      return (IHopMetadataSerializer<T>) existing;
    }
    SharedMetadataSerializer<T> serializer =
        new SharedMetadataSerializer<>(
            this, super.getSerializer(managedClass), store.getSerializer(managedClass));
    serializers.put(managedClass.getName(), serializer);
    return serializer;
  }
}
