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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.compress.compressors.CompressorException;

/**
 * An {@link ICompressionProvider} adapter for a codec that is contributed to {@link
 * HopCompressionRegistry} without a {@link CompressionPlugin} annotated class (for example an
 * extension plugin that only registers a commons-compress {@code CompressorStreamProvider}).
 *
 * <p>The stream operations delegate to the registry; the metadata comes from the {@link
 * CompressionFormat} derived by the factory.
 */
public class RegistryBackedProvider implements ICompressionProvider {

  private final CompressionFormat format;

  public RegistryBackedProvider(CompressionFormat format) {
    this.format = format;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    if (!format.supportsInput()) {
      throw new IOException(
          "Compression format " + format.name() + " does not support decompression");
    }
    try {
      final org.apache.commons.compress.compressors.CompressorInputStream ccIn =
          HopCompressionRegistry.getInstance().createCompressorInputStream(format.id(), in, true);
      return new PlainCompressionInputStream(ccIn, this);
    } catch (CompressorException e) {
      throw new IOException("Cannot create " + format.name() + " input stream", e);
    }
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    if (!format.supportsOutput()) {
      throw new IOException(
          "Compression format " + format.name() + " does not support compression");
    }
    try {
      final org.apache.commons.compress.compressors.CompressorOutputStream<? extends OutputStream>
          ccOut =
              HopCompressionRegistry.getInstance().createCompressorOutputStream(format.id(), out);
      return new PlainCompressionOutputStream(ccOut, this);
    } catch (CompressorException e) {
      throw new IOException("Cannot create " + format.name() + " output stream", e);
    }
  }

  @Override
  public boolean supportsInput() {
    return format.supportsInput();
  }

  @Override
  public boolean supportsOutput() {
    return format.supportsOutput();
  }

  @Override
  public String getName() {
    return format.name();
  }

  @Override
  public String getDescription() {
    return format.description();
  }

  @Override
  public String getDefaultExtension() {
    return format.extension();
  }

  private static final class PlainCompressionInputStream extends CompressionInputStream {
    private PlainCompressionInputStream(
        org.apache.commons.compress.compressors.CompressorInputStream in,
        ICompressionProvider provider) {
      super(in, provider);
    }
  }

  private static final class PlainCompressionOutputStream extends CompressionOutputStream {
    private PlainCompressionOutputStream(
        org.apache.commons.compress.compressors.CompressorOutputStream<? extends OutputStream> out,
        ICompressionProvider provider) {
      super(out, provider);
    }
  }
}
