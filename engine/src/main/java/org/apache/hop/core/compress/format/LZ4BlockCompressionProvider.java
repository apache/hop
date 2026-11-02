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

package org.apache.hop.core.compress.format;

import org.apache.hop.core.compress.CommonsCompressCompressionProvider;
import org.apache.hop.core.compress.CompressionPlugin;

/**
 * Metadata-only compression provider for the LZ4-block format, backed by Apache Commons Compress
 * ({@code CompressorStreamFactory} key "lz4-block").
 */
@CompressionPlugin(
    id = "lz4-block",
    name = "LZ4-block",
    description = "LZ4 block compression",
    classLoaderGroup = "compress-format")
public class LZ4BlockCompressionProvider extends CommonsCompressCompressionProvider {}
