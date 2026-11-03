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

package org.apache.hop.core.compress;

/**
 * Immutable description of a compression format.
 *
 * <p>The metadata is derived from the {@link CompressionPlugin} annotation of the provider class
 * via {@link #of(Class)}; the annotation is the single source of truth shared with the plugin
 * discovery machinery. The {@code id} is the commons-compress {@code CompressorStreamFactory} key
 * (e.g. {@code "zstd"}, {@code "gzip"}, {@code "zip"}, {@code "none"}).
 *
 * @param id the commons-compress registry key of the format
 * @param name the display name; also the legacy artifact reference stored in saved artifacts
 * @param description a human readable description
 * @param extension the default file extension without leading dot, or {@code null} when none
 * @param supportsInput whether the format can decompress (read) data
 * @param supportsOutput whether the format can compress (write) data
 */
public record CompressionFormat(
    String id,
    String name,
    String description,
    String extension,
    boolean supportsInput,
    boolean supportsOutput) {

  /** Normalizes a format id the way commons-compress normalizes registry keys (upper case). */
  public static String normalizeId(String id) {
    return id == null ? "" : id.toUpperCase(java.util.Locale.ROOT);
  }

  /**
   * Builds the format metadata from the {@link CompressionPlugin} annotation of the given class.
   *
   * @param providerClass a class annotated with {@link CompressionPlugin}
   * @return the format metadata
   */
  public static CompressionFormat of(Class<?> providerClass) {
    final CompressionPlugin annotation = providerClass.getAnnotation(CompressionPlugin.class);
    if (annotation == null) {
      throw new IllegalArgumentException(
          "Class " + providerClass.getName() + " is not annotated with @CompressionPlugin");
    }
    final String extension = annotation.defaultExtension();
    return new CompressionFormat(
        annotation.id(),
        annotation.name(),
        annotation.description(),
        extension.isEmpty() ? null : extension,
        annotation.supportsInput(),
        annotation.supportsOutput());
  }
}
