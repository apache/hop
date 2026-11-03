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
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.hop.core.compress.spi.HopCompressorStreamProvider;

/**
 * Base class for the commons-compress backed compression providers.
 *
 * <p>All the metadata (name, description, default extension, input/output support) is declared by
 * the {@link CompressionPlugin} annotation; stream creation is delegated to the Apache Commons
 * Compress registry through {@link HopCompressionRegistry} (which always includes Hop's own {@code
 * none} and {@code zip} SPI codecs from {@link HopCompressorStreamProvider}).
 *
 * <p>For the {@code zip} format the {@link CompressionInputStream#nextEntry()} and {@link
 * CompressionOutputStream#addEntry(String, String)} entry semantics of the historical Hop zip
 * provider are preserved; for all other formats both operations are no-ops, exactly like the legacy
 * behavior.
 */
public abstract class CommonsCompressCompressionProvider implements ICompressionProvider {

  private final CompressionFormat format;

  protected CommonsCompressCompressionProvider() {
    this.format = CompressionFormat.of(getClass());
  }

  /**
   * @return the format metadata derived from this provider's {@link CompressionPlugin}
   */
  public final CompressionFormat getFormat() {
    return format;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    if (!format.supportsInput()) {
      throw new IOException(
          "Compression format " + format.name() + " does not support decompression");
    }
    try {
      final CompressorInputStream ccIn =
          HopCompressionRegistry.getInstance().createCompressorInputStream(format.id(), in, true);
      if (ccIn instanceof HopCompressorStreamProvider.ZipArchiveCompressorInputStream zip) {
        return new ZipEntryCompressionInputStream(ccIn, this, zip);
      }
      return new PlainCompressionInputStream(ccIn, this);
    } catch (CompressorException e) {
      throw new IOException("Cannot create " + format.name() + " input stream", e);
    }
  }

  @Override
  public boolean supportsInput() {
    return format.supportsInput();
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    if (!format.supportsOutput()) {
      throw new IOException(
          "Compression format " + format.name() + " does not support compression");
    }
    try {
      final CompressorOutputStream<? extends OutputStream> ccOut =
          HopCompressionRegistry.getInstance().createCompressorOutputStream(format.id(), out);
      if (ccOut instanceof HopCompressorStreamProvider.ZipArchiveCompressorOutputStream<?> zip) {
        return new ZipEntryCompressionOutputStream(ccOut, this, zip);
      }
      return new PlainCompressionOutputStream(ccOut, this);
    } catch (CompressorException e) {
      throw new IOException("Cannot create " + format.name() + " output stream", e);
    }
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

  /** Hop wrapper over a commons-compress stream without entry semantics. */
  private static final class PlainCompressionInputStream extends CompressionInputStream {
    private PlainCompressionInputStream(CompressorInputStream in, ICompressionProvider provider) {
      super(in, provider);
    }
  }

  /** Hop wrapper over a commons-compress stream without entry semantics. */
  private static final class PlainCompressionOutputStream extends CompressionOutputStream {
    private PlainCompressionOutputStream(
        CompressorOutputStream<? extends OutputStream> out, ICompressionProvider provider) {
      super(out, provider);
    }
  }

  /** Hop wrapper preserving the zip {@code nextEntry()} contract. */
  private static final class ZipEntryCompressionInputStream extends CompressionInputStream {
    private final HopCompressorStreamProvider.ZipArchiveCompressorInputStream zip;

    private ZipEntryCompressionInputStream(
        CompressorInputStream in,
        ICompressionProvider provider,
        HopCompressorStreamProvider.ZipArchiveCompressorInputStream zip) {
      super(in, provider);
      this.zip = zip;
    }

    @Override
    public Object nextEntry() throws IOException {
      return zip.nextEntry();
    }
  }

  /** Hop wrapper preserving the zip {@code addEntry(filename, extension)} contract. */
  private static final class ZipEntryCompressionOutputStream extends CompressionOutputStream {
    private final HopCompressorStreamProvider.ZipArchiveCompressorOutputStream<?> zip;

    private ZipEntryCompressionOutputStream(
        CompressorOutputStream<? extends OutputStream> out,
        ICompressionProvider provider,
        HopCompressorStreamProvider.ZipArchiveCompressorOutputStream<?> zip) {
      super(out, provider);
      this.zip = zip;
    }

    @Override
    public void addEntry(String filename, String extension) throws IOException {
      zip.addEntry(filename, extension);
    }
  }
}
