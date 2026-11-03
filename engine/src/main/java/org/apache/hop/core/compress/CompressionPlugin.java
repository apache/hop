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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation signals to the plugin system that the class is a compression provider plugin.
 *
 * <p>The metadata declared here (default file extension, input/output support) is the single source
 * of truth for the commons-compress backed providers: it is read by {@link
 * org.apache.hop.core.compress.CompressionFormat#of(Class)} and honored by the {@link
 * org.apache.hop.core.compress.CompressionProviderFactory}. External (plugin) providers built on
 * {@link org.apache.hop.core.compress.ICompressionProvider} may keep implementing the interface
 * methods directly.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface CompressionPlugin {

  /**
   * The unique identifier of the provider. For commons-compress backed providers this must be the
   * commons-compress {@code CompressorStreamFactory} key (e.g. {@code "zstd"}, {@code "gzip"},
   * {@code "zip"}, {@code "none"}).
   */
  String id();

  /** The display name of the provider. Used as the reference stored in saved artifacts. */
  String name();

  String description() default "Compression Plugin";

  /**
   * The default file extension associated with this format, without the leading dot (e.g. {@code
   * "gz"}, {@code "zip"}). Empty string means the format has no default extension.
   */
  String defaultExtension() default "";

  /** Whether this provider can decompress (read) data. */
  boolean supportsInput() default true;

  /** Whether this provider can compress (write) data. */
  boolean supportsOutput() default true;

  /**
   * @return True if a separate class loader is needed every time this class is instantiated
   */
  boolean isSeparateClassLoaderNeeded() default false;

  String documentationUrl() default "";

  String casesUrl() default "";

  String forumUrl() default "";

  String classLoaderGroup() default "";
}
