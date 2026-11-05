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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

/** Moves a metadata object from one provider in a stack to another. */
public final class MetadataMover {

  private MetadataMover() {}

  /**
   * Point {@code metadata} at {@code targetProviderDescription}.
   *
   * @param persisted true when the object already exists under its current name in the source place
   */
  public static MetadataMoveStatus move(
      MultiMetadataProvider multi,
      IHopMetadata metadata,
      String targetProviderDescription,
      boolean persisted)
      throws HopException {
    if (multi == null || metadata == null) {
      throw new HopException("A metadata object and a provider are required");
    }
    IHopMetadataProvider target = multi.findProvider(targetProviderDescription);
    if (target == null) {
      throw new HopException(
          "Hop metadata provider '"
              + targetProviderDescription
              + "' could not be found to save object '"
              + metadata.getName()
              + "'");
    }

    String sourceDescription = metadata.getMetadataProviderName();
    if (StringUtils.isEmpty(sourceDescription)) {
      List<IHopMetadataProvider> providers = HopMetadataUtil.getProviders(multi);
      if (!providers.isEmpty()) {
        sourceDescription = providers.get(providers.size() - 1).getDescription();
      }
    }
    if (targetProviderDescription.equals(sourceDescription)) {
      return MetadataMoveStatus.UNCHANGED;
    }

    if (!persisted || StringUtils.isEmpty(metadata.getName())) {
      metadata.setMetadataProviderName(targetProviderDescription);
      return MetadataMoveStatus.RETARGETED;
    }

    IHopMetadataSerializer<IHopMetadata> targetSerializer =
        target.getSerializer(managedClass(metadata));
    if (targetSerializer.exists(metadata.getName())) {
      return MetadataMoveStatus.COLLISION;
    }

    metadata.setMetadataProviderName(targetProviderDescription);
    multi.getSerializer(managedClass(metadata)).save(metadata);

    IHopMetadataProvider source = multi.findProvider(sourceDescription);
    if (source == null || source == target) {
      return MetadataMoveStatus.MOVED;
    }
    try {
      IHopMetadataSerializer<IHopMetadata> sourceSerializer =
          source.getSerializer(managedClass(metadata));
      if (sourceSerializer.exists(metadata.getName())) {
        sourceSerializer.delete(metadata.getName());
      }
    } catch (Exception e) {
      return MetadataMoveStatus.DELETE_FAILED;
    }
    return MetadataMoveStatus.MOVED;
  }

  @SuppressWarnings("unchecked")
  private static <T extends IHopMetadata> Class<T> managedClass(IHopMetadata metadata) {
    return (Class<T>) metadata.getClass();
  }
}
