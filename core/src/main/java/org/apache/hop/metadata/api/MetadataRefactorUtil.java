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

package org.apache.hop.metadata.api;

import org.apache.hop.core.exception.HopException;

/**
 * Utility to link metadata elements to their {@link HopMetadataPropertyType} for refactoring (e.g.
 * rename) so references in pipelines and workflows can be found and updated.
 */
public final class MetadataRefactorUtil {

  private MetadataRefactorUtil() {}

  /**
   * Returns the property type for a metadata type key, if the metadata class declares it via {@link
   * HopMetadata#hopMetadataPropertyType()}. This links the metadata element to the kind of
   * reference used in pipeline/workflow XML (e.g. RDBMS_CONNECTION for "rdbms").
   *
   * @param provider metadata provider (used to resolve the class for the key)
   * @param metadataKey the metadata type key (e.g. "rdbms", "restconnection")
   * @return the property type, or {@link HopMetadataPropertyType#NONE} if not declared or unknown
   */
  public static HopMetadataPropertyType getPropertyTypeForMetadataKey(
      IHopMetadataProvider provider, String metadataKey) {
    try {
      Class<? extends IHopMetadata> clazz = provider.getMetadataClassForKey(metadataKey);
      HopMetadata annotation = clazz.getAnnotation(HopMetadata.class);
      if (annotation == null) {
        return HopMetadataPropertyType.NONE;
      }
      HopMetadataPropertyType type = annotation.hopMetadataPropertyType();
      return type == null ? HopMetadataPropertyType.NONE : type;
    } catch (HopException e) {
      return HopMetadataPropertyType.NONE;
    }
  }

  /**
   * Returns whether the metadata type supports global replace (finding and updating references in
   * pipelines and workflows when renaming). Uses {@link HopMetadata#supportsGlobalReplace()}.
   *
   * @param provider metadata provider (used to resolve the class for the key)
   * @param metadataKey the metadata type key (e.g. "rdbms", "restconnection")
   * @return true if global replace is supported, false otherwise
   */
  public static boolean supportsGlobalReplace(IHopMetadataProvider provider, String metadataKey) {
    try {
      Class<? extends IHopMetadata> clazz = provider.getMetadataClassForKey(metadataKey);
      HopMetadata annotation = clazz.getAnnotation(HopMetadata.class);
      return annotation != null && annotation.supportsGlobalReplace();
    } catch (HopException e) {
      return false;
    }
  }
}
