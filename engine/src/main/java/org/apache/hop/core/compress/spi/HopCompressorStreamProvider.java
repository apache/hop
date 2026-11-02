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

package org.apache.hop.core.compress.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamProvider;

/**
 * Hop's contribution to the Apache Commons Compress {@link CompressorStreamProvider} SPI.
 *
 * <p>This provider is discovered by {@link
 * org.apache.commons.compress.compressors.CompressorStreamFactory} through the {@code
 * META-INF/services} registration, making the Hop-specific {@code none} (passthrough) and {@code
 * zip} (archive with entry support, UTF-8 encoded names) codecs first-class citizens of the
 * commons-compress registry.
 *
 * <p>All other formats (zstd, gzip, bzip2, xz, lzma, snappy-framed/raw, lz4-framed/block, deflate,
 * deflate64, z, pack200, brotli) are provided by the commons-compress built-in factory.
 */
public final class HopCompressorStreamProvider implements CompressorStreamProvider {

  /** The commons-compress registry key of the passthrough (no compression) codec. */
  public static final String NONE = "none";

  /** The commons-compress registry key of the Zip archive codec with entry support. */
  public static final String ZIP = "zip";

  private static final Set<String> INPUT_NAMES = Set.of(NONE, ZIP);
  private static final Set<String> OUTPUT_NAMES = Set.of(NONE, ZIP);

  @Override
  public CompressorInputStream createCompressorInputStream(
      String name, InputStream in, boolean decompressUntilEof) throws CompressorException {
    final String key = keyOf(name);
    switch (key) {
      case NONE:
        return new NoneCompressorInputStream(in);
      case ZIP:
        try {
          return new ZipArchiveCompressorInputStream(in);
        } catch (IOException e) {
          throw new CompressorException("Cannot create Zip input stream", e);
        }
      default:
        throw new CompressorException("Compressor: " + name + " not found");
    }
  }

  @Override
  public <T extends OutputStream> CompressorOutputStream<T> createCompressorOutputStream(
      String name, T out) throws CompressorException {
    final String key = keyOf(name);
    switch (key) {
      case NONE:
        return new NoneCompressorOutputStream<>(out);
      case ZIP:
        final ZipArchiveOutputStream zip = new ZipArchiveOutputStream(out);
        zip.setEncoding(StandardCharsets.UTF_8.name());
        return new ZipArchiveCompressorOutputStream<>(zip);
      default:
        throw new CompressorException("Compressor: " + name + " not found");
    }
  }

  @Override
  public Set<String> getInputStreamCompressorNames() {
    return INPUT_NAMES;
  }

  @Override
  public Set<String> getOutputStreamCompressorNames() {
    return OUTPUT_NAMES;
  }

  private static String keyOf(String name) {
    return name == null ? "" : name.toLowerCase(Locale.ROOT);
  }

  /** Passthrough decompressor: returns the wrapped stream unchanged. */
  private static final class NoneCompressorInputStream extends CompressorInputStream {
    private final InputStream in;

    private NoneCompressorInputStream(InputStream in) {
      this.in = in;
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public int available() throws IOException {
      return in.available();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public boolean markSupported() {
      return in.markSupported();
    }

    @Override
    public synchronized void mark(int readlimit) {
      in.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
      in.reset();
    }
  }

  /** Passthrough compressor: writes to the wrapped stream unchanged. */
  private static final class NoneCompressorOutputStream<T extends OutputStream>
      extends CompressorOutputStream<T> {
    private final T out;

    private NoneCompressorOutputStream(T out) {
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
  }

  /**
   * Adapts {@link ZipArchiveInputStream} to the commons-compress {@link CompressorInputStream}
   * contract. {@link #nextEntry()} exposes the archive entry semantics used by Hop consumers.
   */
  public static final class ZipArchiveCompressorInputStream extends CompressorInputStream {
    private final ZipArchiveInputStream zip;

    public ZipArchiveCompressorInputStream(InputStream in) throws IOException {
      this.zip = new ZipArchiveInputStream(in, StandardCharsets.UTF_8.name());
    }

    /** Positions the stream on the next archive entry, {@code null} at end of archive. */
    public Object nextEntry() throws IOException {
      return zip.getNextEntry();
    }

    @Override
    public int read() throws IOException {
      return zip.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return zip.read(b, off, len);
    }

    @Override
    public int available() throws IOException {
      return zip.available();
    }

    @Override
    public void close() throws IOException {
      zip.close();
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public synchronized void mark(int readlimit) {
      // not supported
    }

    @Override
    public synchronized void reset() throws IOException {
      throw new IOException("mark/reset not supported by ZipArchiveInputStream");
    }
  }

  /**
   * Adapts {@link ZipArchiveOutputStream} to the commons-compress {@link CompressorOutputStream}
   * contract. The archive entry semantics ({@link #addEntry(String, String)}) are exposed so that
   * Hop wrappers can keep the {@code addEntry(filename, extension)} contract unchanged; the
   * contract is otherwise identical to the historical Hop zip provider for wire compatibility.
   */
  public static final class ZipArchiveCompressorOutputStream<T extends OutputStream>
      extends CompressorOutputStream<T> {
    private final ZipArchiveOutputStream zip;
    private boolean entryOpen;

    public ZipArchiveCompressorOutputStream(ZipArchiveOutputStream zip) {
      super(null);
      this.zip = zip;
    }

    /** Adds a new archive entry using the historical Hop entry-path derivation. */
    public void addEntry(String filename, String extension) throws IOException {
      zip.putArchiveEntry(new ZipArchiveEntry(hopEntryPath(filename, extension)));
      entryOpen = true;
    }

    /** Finishes the current archive entry, if one is open. */
    public void closeEntry() throws IOException {
      if (entryOpen) {
        zip.closeArchiveEntry();
        entryOpen = false;
      }
    }

    @Override
    public void write(int b) throws IOException {
      zip.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      zip.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      zip.flush();
    }

    @Override
    public void close() throws IOException {
      closeEntry();
      zip.close();
    }
  }

  /**
   * Builds the zip entry path the same way the historical Hop zip provider did: strip the folder
   * hierarchy and a trailing {@code .zip} extension, then append the real extension when given.
   */
  static String hopEntryPath(String filename, String extension) {
    if (filename == null) {
      filename = "";
    }
    int index = filename.lastIndexOf('/');
    String entryPath = index != -1 ? filename.substring(index + 1) : filename;
    index = entryPath.toLowerCase(Locale.ROOT).lastIndexOf(".zip");
    if (index != -1) {
      entryPath = entryPath.substring(0, index) + entryPath.substring(index + ".zip".length());
    }
    if (extension != null && !extension.isEmpty()) {
      entryPath += "." + extension;
    }
    return entryPath;
  }
}
