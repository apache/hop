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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileRecordScanner.Start;

/**
 * Finds the exact record starts to split a file at when breaks in enclosed fields are allowed, so a
 * line feed inside an enclosed field never starts a chunk. It reads the file twice in total: once
 * here, the pieces in parallel, and once more when the copies read the chunks.
 *
 * <p>Each piece is scanned from its first line feed on, from each of the three states possible
 * there (see {@link TextFileRecordScanner}). Chaining the pieces from the start of the file, where
 * a record starts, then tells which of the three each piece really starts in, and so where its
 * first record starts.
 */
final class TextFileRecordSplitter {

  private static final int LF = 0x0A;
  private static final int BUFFER_SIZE = 256 * 1024;

  /**
   * How records end when breaks in enclosed fields are allowed, as Text File Input reads them.
   *
   * @param enclosure the enclosure, empty for none
   * @param escape the escape character, empty for none
   * @param fileFormat {@link org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil}'s file
   *     format number
   * @param noEmptyLines empty lines are left out, also when counting header lines
   */
  record RecordFormat(String enclosure, String escape, int fileFormat, boolean noEmptyLines) {

    TextFileRecordScanner scanner() {
      return new TextFileRecordScanner(units(enclosure), units(escape), fileFormat);
    }

    private static int[] units(String text) {
      return text == null ? new int[0] : text.chars().toArray();
    }
  }

  private TextFileRecordSplitter() {}

  /**
   * Why a file in this encoding with this format can't be split at exact record starts, or null
   * when it can: the enclosure and escape characters must be ASCII, in an encoding where an ASCII
   * byte (or UTF-16 unit) is always that character.
   */
  static String problem(Charset charset, RecordFormat format) {
    String name = charset.name().toUpperCase(Locale.ROOT);
    boolean suitable =
        name.equals("UTF-8")
            || name.equals("US-ASCII")
            || name.startsWith("ISO-8859-")
            || name.startsWith("WINDOWS-125")
            || name.equals("UTF-16LE")
            || name.equals("UTF-16BE")
            || name.equals("UTF-16")
            || name.equals("X-UTF-16LE-BOM");
    if (!suitable) {
      return "breaks in enclosed fields can't be found by bytes in encoding " + charset.name();
    }
    for (String text : new String[] {format.enclosure(), format.escape()}) {
      if (text != null && !text.chars().allMatch(c -> c < 0x80 && c != '\r' && c != '\n')) {
        return "breaks in enclosed fields can only be found with an enclosure and escape"
            + " character in plain ASCII";
      }
    }
    return null;
  }

  /** Reads units: bytes, or the 16-bit units of UTF-16 in the byte order of its line feed. */
  static final class UnitReader implements AutoCloseable {
    private final InputStream in;
    private final int width;
    private final boolean littleEndian;
    private long position;

    UnitReader(FileObject file, long offset, byte[] lineFeed) throws IOException {
      this(file, offset, lineFeed, TextFileReadAhead.NONE, offset);
    }

    /** Reads from the offset, fetching ahead up to {@code readAheadEnd}. */
    UnitReader(
        FileObject file,
        long offset,
        byte[] lineFeed,
        TextFileReadAhead readAhead,
        long readAheadEnd)
        throws IOException {
      InputStream source =
          readAhead.blocks() < 1
              ? HopVfs.getInputStream(file, offset)
              : readAhead.open(file, offset, readAheadEnd);
      this.in = new BufferedInputStream(source, BUFFER_SIZE);
      this.width = lineFeed.length;
      this.littleEndian = lineFeed[0] == LF;
      this.position = offset;
    }

    /** The next unit, or -1 at the end of the file. */
    int read() throws IOException {
      int first = in.read();
      if (first < 0) {
        return -1;
      }
      if (width == 1) {
        position++;
        return first;
      }
      int second = in.read();
      if (second < 0) {
        return -1;
      }
      position += 2;
      return littleEndian ? first | (second << 8) : (first << 8) | second;
    }

    /** The position after the last unit read. */
    long position() {
      return position;
    }

    int width() {
      return width;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  /**
   * Where the data starts after the header records, counting records as Text File Input does.
   *
   * @return the position after the last header record, or -1 when that's beyond {@code limit}
   */
  static long headerEnd(
      FileObject file,
      long start,
      byte[] lineFeed,
      RecordFormat format,
      int headerLines,
      long limit)
      throws IOException {
    TextFileRecordScanner scanner = format.scanner();
    scanner.start(Start.RECORD);
    int found = 0;
    try (UnitReader in = new UnitReader(file, start, lineFeed)) {
      while (found < headerLines) {
        if (in.position() > limit) {
          return -1;
        }
        int unit = in.read();
        if (unit < 0) {
          // The file is all header
          break;
        }
        if (scanner.feed(unit) && (!format.noEmptyLines() || scanner.recordHadCharacters())) {
          found++;
        }
      }
      return in.position();
    }
  }

  /** What scanning one piece from each of the three possible start states found. */
  private record Piece(long start, Start[] endState, long[] firstRecordStart) {

    boolean isEmpty() {
      return endState == null;
    }
  }

  /**
   * The record starts to split the file at, from the start of the file on: 0 first, then a start in
   * or after every cut, as long as a record starts there.
   *
   * @param dataStart where the first record after the header starts
   * @param size the size of the file
   * @param split the distance between the cuts
   * @param readAhead how each piece is fetched ahead, up to the next cut
   * @param pool the threads that scan the pieces
   */
  static List<Long> recordStarts(
      FileObject file,
      long dataStart,
      long size,
      long split,
      byte[] lineFeed,
      RecordFormat format,
      TextFileReadAhead readAhead,
      ExecutorService pool)
      throws IOException {
    List<Long> cuts = new ArrayList<>();
    cuts.add(dataStart);
    for (long cut = split; cut < size; cut += split) {
      if (cut > dataStart) {
        cuts.add(cut);
      }
    }

    List<Future<Piece>> pieces = new ArrayList<>();
    for (int k = 0; k < cuts.size(); k++) {
      long from = cuts.get(k);
      long until = k + 1 < cuts.size() ? cuts.get(k + 1) : Long.MAX_VALUE;
      boolean first = k == 0;
      Callable<Piece> scan = () -> scan(file, from, until, first, lineFeed, format, readAhead);
      pieces.add(pool.submit(scan));
    }

    List<Long> starts = new ArrayList<>();
    starts.add(0L);
    Start state = Start.RECORD;
    for (int k = 0; k < pieces.size(); k++) {
      Piece piece = result(pieces.get(k));
      if (piece.isEmpty()) {
        continue;
      }
      long recordStart =
          state == Start.RECORD ? piece.start() : piece.firstRecordStart()[state.ordinal()];
      // A record that starts right after a piece is found by both that piece and the next
      if (k > 0 && recordStart > starts.get(starts.size() - 1) && recordStart < size) {
        starts.add(recordStart);
      }
      state = piece.endState()[state.ordinal()];
    }
    return starts;
  }

  private static Piece result(Future<Piece> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while splitting a file", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException io) {
        throw io;
      }
      throw new IOException("Unable to split a file", e.getCause());
    }
  }

  /**
   * Scans the piece from the first line feed at or after {@code from} (or from {@code from} itself
   * for the first piece) to the first line feed at or after {@code until}.
   */
  private static Piece scan(
      FileObject file,
      long from,
      long until,
      boolean first,
      byte[] lineFeed,
      RecordFormat format,
      TextFileReadAhead readAhead)
      throws IOException {
    Start[] states = Start.values();
    try (UnitReader in = new UnitReader(file, from, lineFeed, readAhead, until)) {
      if (!first) {
        long lineFeedAt;
        int unit;
        // Stop at the next cut: a line feed from there on is the next piece's to find. A record
        // much longer than the pieces would otherwise be read on to its end by every piece in it.
        do {
          lineFeedAt = in.position();
          unit = in.read();
        } while (unit >= 0 && unit != LF && in.position() < until);
        if (unit != LF || lineFeedAt >= until) {
          // The next piece starts after the same line feed, or the file ends first
          return new Piece(in.position(), null, null);
        }
      }
      long start = in.position();

      TextFileRecordScanner[] scanners = new TextFileRecordScanner[states.length];
      long[] firstRecordStart = new long[states.length];
      for (Start state : states) {
        scanners[state.ordinal()] = format.scanner();
        scanners[state.ordinal()].start(state);
        firstRecordStart[state.ordinal()] = -1;
      }
      while (true) {
        long unitAt = in.position();
        int unit = in.read();
        if (unit < 0) {
          break;
        }
        for (int s = 0; s < scanners.length; s++) {
          if (scanners[s].feed(unit) && firstRecordStart[s] < 0) {
            firstRecordStart[s] = in.position();
          }
        }
        if (unit == LF && unitAt >= until) {
          break;
        }
      }
      Start[] endState = new Start[states.length];
      for (int s = 0; s < scanners.length; s++) {
        endState[s] = scanners[s].state();
      }
      return new Piece(start, endState, firstRecordStart);
    }
  }

  /** Threads for scanning the pieces of the files, daemons so they never keep Hop running. */
  static ExecutorService newPool(int threads) {
    return Executors.newFixedThreadPool(
        Math.max(1, threads),
        task -> {
          Thread thread = new Thread(task, "Text File Input split");
          thread.setDaemon(true);
          return thread;
        });
  }
}
