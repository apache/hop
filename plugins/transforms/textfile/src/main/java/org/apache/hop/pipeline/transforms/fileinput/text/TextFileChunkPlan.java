/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.fileinput.text;

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileRecordSplitter.RecordFormat;

/**
 * The chunks the copies of one Text File Input read when it runs in parallel, shared by those
 * copies: each takes the next chunk until none are left, so a fast copy reads more of them.
 *
 * <p>The split size is the chunk size, or less when there is little data, so that every copy gets
 * work: {@code min(chunk size, max(4 MB, total size / copies))}. A file is read whole by one copy
 * when it is no bigger than that, when its storage can't start reading at an offset, or when its
 * encoding has no line feed that can be found by its bytes alone.
 *
 * <p>With breaks in enclosed fields allowed, a line feed doesn't always start a record. The files
 * are then read once first, in parallel, to find the exact record starts to split at (see {@link
 * TextFileRecordSplitter}).
 */
public final class TextFileChunkPlan {

  public static final long DEFAULT_CHUNK_SIZE = 128L * 1024 * 1024;

  /** {@link #DEFAULT_CHUNK_SIZE} as a new transform shows it. */
  public static final String DEFAULT_CHUNK_SIZE_TEXT = "128MB";

  static final long MIN_SPLIT_SIZE = 4L * 1024 * 1024;

  /** How far into a file the header lines may reach before the file is read whole instead. */
  static final int MAX_PROLOGUE = 1024 * 1024;

  private final Queue<TextFileChunk> chunks;
  private final int size;

  private TextFileChunkPlan(List<TextFileChunk> chunks) {
    this.chunks = new ConcurrentLinkedQueue<>(chunks);
    this.size = chunks.size();
  }

  /** The next chunk to read, or null when every chunk is taken. Safe to call from every copy. */
  public TextFileChunk next() {
    return chunks.poll();
  }

  /** How many chunks the plan started with. */
  public int size() {
    return size;
  }

  /** The split size for the given total size: the chunk size, or less when there is little data. */
  static long splitSize(long chunkSize, long totalSize, int copies) {
    long perCopy = (totalSize + copies - 1) / Math.max(1, copies);
    long split = Math.min(chunkSize, Math.max(MIN_SPLIT_SIZE, perCopy));
    // Even, so that two-byte (UTF-16) line feeds never straddle a cut
    return Math.max(2, split + (split % 2));
  }

  /**
   * Parses a chunk size such as {@code 128MB}, {@code 512 KB}, {@code 1GB} or a number of bytes.
   * Blank means the default.
   */
  public static long parseChunkSize(String value) throws HopException {
    if (StringUtils.isBlank(value)) {
      return DEFAULT_CHUNK_SIZE;
    }
    String text = value.trim().toUpperCase(Locale.ROOT).replace(" ", "");
    long multiplier = 1;
    for (String[] suffix :
        new String[][] {{"GB", "1073741824"}, {"MB", "1048576"}, {"KB", "1024"}, {"B", "1"}}) {
      if (text.endsWith(suffix[0])) {
        text = text.substring(0, text.length() - suffix[0].length());
        multiplier = Long.parseLong(suffix[1]);
        break;
      }
    }
    try {
      long bytes = Long.parseLong(text) * multiplier;
      if (bytes <= 0) {
        throw new NumberFormatException();
      }
      return bytes;
    } catch (NumberFormatException e) {
      throw new HopException(
          "Invalid chunk size '" + value + "': use a number of bytes or a size like 64MB");
    }
  }

  /**
   * A file to read, with the row of the previous transform that named it.
   *
   * @param file the file
   * @param passThruRow the row whose fields are passed through to every row read from the file;
   *     null when the files don't come from a previous transform
   */
  public record Source(FileObject file, Object[] passThruRow) {}

  /**
   * Splits files from the transform's own file list into chunks, as {@link #buildFromSources} does.
   */
  public static TextFileChunkPlan build(
      List<FileObject> files,
      long chunkSize,
      int copies,
      boolean header,
      int headerLines,
      String encoding,
      RecordFormat records,
      ILogChannel log)
      throws IOException {
    return buildFromSources(
        files.stream().map(file -> new Source(file, null)).collect(Collectors.toList()),
        chunkSize,
        copies,
        header,
        headerLines,
        encoding,
        records,
        TextFileReadAhead.NONE,
        log);
  }

  /**
   * The split size for one file. A chunk smaller than the block its storage reads in would still
   * download whole blocks, so the split is at least the storage's read block size. A configured
   * chunk size below it is not refused: it is logged once per block size, and the block size used.
   *
   * @param split the split size of the plan
   * @param chunkSize the configured chunk size
   * @param blockSize the read block size the file's storage reports, 0 for none
   * @param warned the block sizes already warned about in this plan
   */
  static long splitForStorage(
      long split,
      long chunkSize,
      int blockSize,
      FileObject file,
      Set<Integer> warned,
      ILogChannel log) {
    if (blockSize <= 0 || split >= blockSize) {
      return split;
    }
    if (chunkSize < blockSize && warned.add(blockSize)) {
      log.logBasic(
          "Warning: the chunk size of "
              + chunkSize
              + " bytes is smaller than the read block size of "
              + blockSize
              + " bytes of the storage of "
              + file.getName().getFriendlyURI()
              + ", which would download whole blocks for every chunk: using chunks of "
              + blockSize
              + " bytes instead");
    }
    // Even, so that two-byte (UTF-16) line feeds never straddle a cut
    return blockSize + (blockSize % 2);
  }

  /**
   * The size of a file, or 0 when it doesn't exist. A missing file named by a previous transform
   * becomes a whole-file chunk, so that the copy opening it reports or skips it the way it does
   * without parallel reading.
   */
  private static long sizeOf(FileObject file) throws IOException {
    try {
      return file.getContent().getSize();
    } catch (FileSystemException e) {
      if (!file.exists()) {
        return 0;
      }
      throw e;
    }
  }

  /**
   * Splits the files into chunks. The same file listed twice is read twice, once per entry, as it
   * is without parallel reading.
   *
   * @param sources the files, in the order they are read
   * @param chunkSize the largest chunk, in bytes
   * @param copies the number of copies reading the chunks
   * @param header whether the files start with header lines
   * @param headerLines how many
   * @param encoding the configured encoding, blank for the platform default
   * @param records how records end when breaks in enclosed fields are allowed, null when every line
   *     feed ends a record
   * @param readAhead how the files are fetched ahead when they are read once first, to find the
   *     record starts with breaks in enclosed fields allowed
   * @param log where to say which files are read whole and why
   */
  public static TextFileChunkPlan buildFromSources(
      List<Source> sources,
      long chunkSize,
      int copies,
      boolean header,
      int headerLines,
      String encoding,
      RecordFormat records,
      TextFileReadAhead readAhead,
      ILogChannel log)
      throws IOException {
    return buildFromSources(
        sources,
        chunkSize,
        copies,
        header,
        headerLines,
        encoding,
        records,
        readAhead,
        log,
        HopVfs::getStorageReadBlockSize);
  }

  /**
   * As above, with the read block size of each file's storage from {@code storageBlockSize}: 0 when
   * the storage reports none.
   */
  static TextFileChunkPlan buildFromSources(
      List<Source> sources,
      long chunkSize,
      int copies,
      boolean header,
      int headerLines,
      String encoding,
      RecordFormat records,
      TextFileReadAhead readAhead,
      ILogChannel log,
      ToIntFunction<FileObject> storageBlockSize)
      throws IOException {
    List<FileObject> files = sources.stream().map(Source::file).collect(Collectors.toList());
    long[] sizes = new long[files.size()];
    long total = 0;
    for (int i = 0; i < files.size(); i++) {
      sizes[i] = sizeOf(files.get(i));
      total += sizes[i];
    }
    long split = splitSize(chunkSize, total, copies);

    List<TextFileChunk> chunks = new ArrayList<>();
    Set<Integer> warned = new HashSet<>();
    ExecutorService pool = records == null ? null : TextFileRecordSplitter.newPool(copies);
    try {
      for (int i = 0; i < files.size(); i++) {
        FileObject file = files.get(i);
        Object[] passThruRow = sources.get(i).passThruRow();
        long size = sizes[i];
        long fileSplit =
            splitForStorage(split, chunkSize, storageBlockSize.applyAsInt(file), file, warned, log);
        if (size <= fileSplit) {
          chunks.add(TextFileChunk.whole(file, size, passThruRow));
          continue;
        }
        if (!HopVfs.supportsRangeRead(file)) {
          log.logBasic(
              "Reading "
                  + file.getName().getFriendlyURI()
                  + " in one piece: its storage can't start reading in the middle of a file");
          chunks.add(TextFileChunk.whole(file, size, passThruRow));
          continue;
        }
        Prologue prologue = Prologue.probe(file, header, headerLines, encoding, records);
        if (prologue.reason() != null) {
          log.logBasic(
              "Reading " + file.getName().getFriendlyURI() + " in one piece: " + prologue.reason());
          chunks.add(TextFileChunk.whole(file, size, passThruRow));
          continue;
        }
        if (records != null) {
          List<Long> starts =
              TextFileRecordSplitter.recordStarts(
                  file,
                  prologue.dataStart(),
                  size,
                  fileSplit,
                  prologue.lineFeed(),
                  records,
                  readAhead,
                  pool);
          for (int s = 0; s < starts.size(); s++) {
            long chunkEnd = s + 1 < starts.size() ? starts.get(s + 1) : size;
            chunks.add(
                new TextFileChunk(
                    file,
                    starts.get(s),
                    chunkEnd,
                    false,
                    prologue.charset(),
                    prologue.lineFeed(),
                    true,
                    passThruRow));
          }
          continue;
        }
        for (long cut = 0; cut < size; cut += fileSplit) {
          long chunkEnd = Math.min(cut + fileSplit, size);
          // A chunk inside the file starts after the header, which the first chunk skips
          long chunkStart = cut == 0 ? 0 : Math.max(cut, prologue.dataStart());
          if (chunkStart < chunkEnd) {
            chunks.add(
                new TextFileChunk(
                    file,
                    chunkStart,
                    chunkEnd,
                    false,
                    prologue.charset(),
                    prologue.lineFeed(),
                    false,
                    passThruRow));
          }
        }
      }
    } finally {
      if (pool != null) {
        pool.shutdownNow();
      }
    }
    if (log.isDetailed()) {
      log.logDetailed(
          "Reading "
              + files.size()
              + " file(s) in "
              + chunks.size()
              + " chunk(s) of up to "
              + split
              + " bytes");
    }
    return new TextFileChunkPlan(chunks);
  }

  /**
   * The line feed of an encoding when lines can be found by their bytes: one 0x0A byte that never
   * occurs inside another character (UTF-8, ISO-8859-x, windows-125x, the CJK double-byte sets), or
   * the two bytes of UTF-16. Null otherwise.
   */
  static byte[] lineFeed(Charset charset) {
    String name = charset.name().toUpperCase(Locale.ROOT);
    if (name.equals("UTF-16LE") || name.equals("X-UTF-16LE-BOM")) {
      return new byte[] {0x0A, 0x00};
    }
    if (name.equals("UTF-16BE") || name.equals("UTF-16")) {
      return new byte[] {0x00, 0x0A};
    }
    if (name.startsWith("ISO-2022") || name.startsWith("UTF-32") || name.startsWith("X-UTF-32")) {
      // Shift states, or four-byte units
      return null;
    }
    byte[] newline = "\n".getBytes(charset);
    byte[] carriageReturn = "\r".getBytes(charset);
    byte[] letter = "a".getBytes(charset);
    if (Arrays.equals(newline, new byte[] {0x0A})
        && Arrays.equals(carriageReturn, new byte[] {0x0D})
        && Arrays.equals(letter, new byte[] {0x61})) {
      return new byte[] {0x0A};
    }
    return null;
  }

  /**
   * What the start of a file says about reading it from the middle: its encoding (a byte order mark
   * overrides the configured one, as for a whole file) and where the data starts after the header.
   *
   * @param reason why the file can't be split, or null when it can
   */
  record Prologue(String charset, byte[] lineFeed, long dataStart, String reason) {

    static Prologue unsplittable(String reason) {
      return new Prologue(null, null, 0, reason);
    }

    static Prologue probe(
        FileObject file, boolean header, int headerLines, String encoding, RecordFormat records)
        throws IOException {
      try (BufferedInputStream in =
          new BufferedInputStream(HopVfs.getInputStream(file, 0), 64 * 1024)) {
        BOMDetector bom = new BOMDetector(in);
        Charset charset;
        if (bom.bomExist()) {
          charset = Charset.forName(bom.getCharset());
        } else if (StringUtils.isNotEmpty(encoding)) {
          charset = Charset.forName(encoding);
        } else {
          charset = Charset.defaultCharset();
        }
        byte[] lineFeed = TextFileChunkPlan.lineFeed(charset);
        if (lineFeed == null) {
          return unsplittable("lines in encoding " + charset.name() + " can't be found by bytes");
        }
        long dataStart = bom.bomExist() ? bom.getBomSize() : 0;
        if (records != null) {
          String problem = TextFileRecordSplitter.problem(charset, records);
          if (problem != null) {
            return unsplittable(problem);
          }
          if (header && headerLines > 0) {
            dataStart =
                TextFileRecordSplitter.headerEnd(
                    file, dataStart, lineFeed, records, headerLines, MAX_PROLOGUE);
            if (dataStart < 0) {
              return unsplittable("its header lines are longer than " + MAX_PROLOGUE + " bytes");
            }
          }
        } else if (header && headerLines > 0) {
          int found = 0;
          byte[] unit = new byte[lineFeed.length];
          while (found < headerLines) {
            if (dataStart > MAX_PROLOGUE) {
              return unsplittable("its header lines are longer than " + MAX_PROLOGUE + " bytes");
            }
            int read = in.readNBytes(unit, 0, unit.length);
            if (read < unit.length) {
              // The file is all header
              break;
            }
            dataStart += unit.length;
            if (Arrays.equals(unit, lineFeed)) {
              found++;
            }
          }
        }
        return new Prologue(charset.name(), lineFeed, dataStart, null);
      }
    }
  }

  /**
   * Reads one chunk: the bytes of its lines, from the storage of the file, fetched ahead up to the
   * end of the chunk.
   */
  static InputStream open(TextFileChunk chunk, TextFileReadAhead readAhead) throws IOException {
    if (chunk.exact()) {
      return new BoundedInputStream(
          readAhead.open(chunk.file(), chunk.start(), chunk.end()), chunk.end() - chunk.start());
    }
    return TextFileChunkInputStream.open(
        chunk.file(), chunk.start(), chunk.end(), chunk.lineFeed(), readAhead);
  }

  /** The first bytes of a stream only. */
  private static final class BoundedInputStream extends FilterInputStream {
    private long remaining;

    BoundedInputStream(InputStream in, long length) {
      super(in);
      this.remaining = length;
    }

    @Override
    public int read() throws IOException {
      if (remaining <= 0) {
        return -1;
      }
      int b = super.read();
      if (b >= 0) {
        remaining--;
      }
      return b;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      if (remaining <= 0) {
        return -1;
      }
      int read = super.read(buffer, offset, (int) Math.min(length, remaining));
      if (read > 0) {
        remaining -= read;
      }
      return read;
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = super.skip(Math.min(n, remaining));
      remaining -= skipped;
      return skipped;
    }

    @Override
    public int available() throws IOException {
      return (int) Math.min(super.available(), remaining);
    }

    @Override
    public boolean markSupported() {
      return false;
    }
  }
}
