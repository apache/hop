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
package org.apache.hop.core.plugins.manifest;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Adds one file to a finished zip without rewriting what is already in it.
 *
 * <p>Every existing entry keeps the bytes the archiver wrote; only the central directory moves, to
 * behind the new entry. The new entry is stored, not compressed, and takes its time from the newest
 * file already in the zip: the DOS date and time and the UTC time of the extended timestamp field
 * are copied, not computed, so nothing depends on the clock or the time zone of the machine that
 * runs this. In a build with {@code project.build.outputTimestamp} every entry carries that time
 * and the zip stays byte for byte reproducible. When that file is a Unix one the entry gets mode
 * {@code rw-r--r--}.
 *
 * <p>An entry with the same name is replaced when it is the last one in the zip, which is where
 * this class puts it, so adding the same file twice gives the same zip as adding it once. Zip64 and
 * multi-disk archives are refused.
 */
final class ZipAppender {

  private static final int LOCAL_HEADER_SIGNATURE = 0x04034b50;
  private static final int CENTRAL_HEADER_SIGNATURE = 0x02014b50;
  private static final int END_SIGNATURE = 0x06054b50;
  private static final int ZIP64_LOCATOR_SIGNATURE = 0x07064b50;
  private static final int LOCAL_HEADER_SIZE = 30;
  private static final int CENTRAL_HEADER_SIZE = 46;
  private static final int END_SIZE = 22;
  private static final int ZIP64_LOCATOR_SIZE = 20;
  private static final int MAX_COMMENT = 0xFFFF;
  private static final int EXTENDED_TIMESTAMP_ID = 0x5455;
  private static final int VERSION_NEEDED_STORED = 10;
  private static final int FLAG_UTF8 = 1 << 11;
  private static final int PLATFORM_UNIX = 3;

  /** Unix, zip specification 2.0: for a zip without any other file to copy from. */
  private static final int DEFAULT_VERSION_MADE_BY = (PLATFORM_UNIX << 8) | 20;

  /** A regular file with mode rw-r--r--, in the upper half of the external attributes. */
  private static final int UNIX_FILE_ATTRIBUTES = 0100644 << 16;

  /** 1980-01-01, the first day a zip can hold, for a zip without any other file. */
  private static final int DEFAULT_DOS_DATE = (1 << 5) | 1;

  private ZipAppender() {}

  /** One record of the central directory, at {@code start} in the directory bytes. */
  private record CentralRecord(
      int start,
      int length,
      String name,
      int versionMadeBy,
      int dosTime,
      int dosDate,
      long localHeaderOffset,
      Integer modifiedTime) {

    boolean isDirectory() {
      return name.endsWith("/");
    }

    long dosDateTime() {
      return ((long) dosDate << 16) | dosTime;
    }
  }

  /**
   * Add {@code content} to {@code zip} as {@code name}.
   *
   * @throws IOException when the zip cannot be read or written, uses Zip64 or spans several disks,
   *     or already holds {@code name} somewhere else than as its last entry
   */
  static void append(Path zip, String name, byte[] content) throws IOException {
    try (FileChannel channel =
        FileChannel.open(zip, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      long size = channel.size();
      int tailLength = (int) Math.min(size, ZIP64_LOCATOR_SIZE + END_SIZE + MAX_COMMENT);
      ByteBuffer tail = read(channel, size - tailLength, tailLength);
      int end = findEnd(tail);
      if (end < 0) {
        throw new IOException(zip + " is not a zip: it has no end of central directory record");
      }
      long endPosition = size - tailLength + end;
      int entries = u16(tail, end + 10);
      long directorySize = u32(tail, end + 12);
      long directoryOffset = u32(tail, end + 16);
      boolean zip64Locator =
          end >= ZIP64_LOCATOR_SIZE
              && tail.getInt(end - ZIP64_LOCATOR_SIZE) == ZIP64_LOCATOR_SIGNATURE;
      if (zip64Locator
          || entries == 0xFFFF
          || directorySize == 0xFFFFFFFFL
          || directoryOffset == 0xFFFFFFFFL) {
        throw new IOException(zip + " is a Zip64 archive: adding " + name + " is not supported");
      }
      if (u16(tail, end + 4) != 0 || u16(tail, end + 6) != 0 || u16(tail, end + 8) != entries) {
        throw new IOException(zip + " spans several disks: adding " + name + " is not supported");
      }
      if (directoryOffset + directorySize != endPosition) {
        throw new IOException(
            zip + ": the central directory does not end where the end of central directory starts");
      }
      byte[] comment = new byte[u16(tail, end + 20)];
      tail.get(end + END_SIZE, comment);

      ByteBuffer directory = read(channel, directoryOffset, (int) directorySize);
      List<CentralRecord> records = parse(directory, entries, zip);

      // The file is replaced when it is the last entry: where an earlier run put it
      long lastLocalHeader = -1;
      for (CentralRecord rec : records) {
        lastLocalHeader = Math.max(lastLocalHeader, rec.localHeaderOffset());
      }
      CentralRecord replaced = null;
      long dataEnd = directoryOffset;
      for (CentralRecord rec : records) {
        if (rec.name().equals(name)) {
          if (rec.localHeaderOffset() != lastLocalHeader) {
            throw new IOException(zip + " already contains " + name + ", not as its last entry");
          }
          replaced = rec;
          dataEnd = rec.localHeaderOffset();
        }
      }

      CentralRecord template = null;
      for (CentralRecord rec : records) {
        if (rec != replaced
            && !rec.isDirectory()
            && (template == null || rec.dosDateTime() > template.dosDateTime())) {
          template = rec;
        }
      }
      int dosTime = template == null ? 0 : template.dosTime();
      int dosDate = template == null ? DEFAULT_DOS_DATE : template.dosDate();
      int versionMadeBy = template == null ? DEFAULT_VERSION_MADE_BY : template.versionMadeBy();
      int externalAttributes = (versionMadeBy >> 8) == PLATFORM_UNIX ? UNIX_FILE_ATTRIBUTES : 0;
      byte[] extra =
          template == null || template.modifiedTime() == null
              ? new byte[0]
              : extendedTimestamp(template.modifiedTime());

      byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
      int flags = name.chars().allMatch(c -> c < 0x80) ? 0 : FLAG_UTF8;
      CRC32 crc = new CRC32();
      crc.update(content);

      int keptDirectorySize = (int) directorySize - (replaced == null ? 0 : replaced.length());
      int newEntries = entries + (replaced == null ? 1 : 0);
      long newDirectoryOffset =
          dataEnd + LOCAL_HEADER_SIZE + nameBytes.length + extra.length + content.length;
      int newDirectorySize =
          keptDirectorySize + CENTRAL_HEADER_SIZE + nameBytes.length + extra.length;
      if (newEntries >= 0xFFFF || newDirectoryOffset + newDirectorySize >= 0xFFFFFFFFL) {
        throw new IOException(
            zip + ": adding " + name + " would need Zip64, which is not supported");
      }

      ByteBuffer out =
          ByteBuffer.allocate(
                  (int) (newDirectoryOffset - dataEnd)
                      + newDirectorySize
                      + END_SIZE
                      + comment.length)
              .order(ByteOrder.LITTLE_ENDIAN);
      // Local file header and data
      out.putInt(LOCAL_HEADER_SIGNATURE)
          .putShort((short) VERSION_NEEDED_STORED)
          .putShort((short) flags)
          .putShort((short) 0) // stored
          .putShort((short) dosTime)
          .putShort((short) dosDate)
          .putInt((int) crc.getValue())
          .putInt(content.length)
          .putInt(content.length)
          .putShort((short) nameBytes.length)
          .putShort((short) extra.length)
          .put(nameBytes)
          .put(extra)
          .put(content);
      // The central directory as it was, without a replaced record, then the new one
      for (CentralRecord rec : records) {
        if (rec != replaced) {
          out.put(directory.slice(rec.start(), rec.length()));
        }
      }
      out.putInt(CENTRAL_HEADER_SIGNATURE)
          .putShort((short) versionMadeBy)
          .putShort((short) VERSION_NEEDED_STORED)
          .putShort((short) flags)
          .putShort((short) 0) // stored
          .putShort((short) dosTime)
          .putShort((short) dosDate)
          .putInt((int) crc.getValue())
          .putInt(content.length)
          .putInt(content.length)
          .putShort((short) nameBytes.length)
          .putShort((short) extra.length)
          .putShort((short) 0) // comment length
          .putShort((short) 0) // disk number
          .putShort((short) 0) // internal attributes
          .putInt(externalAttributes)
          .putInt((int) dataEnd)
          .put(nameBytes)
          .put(extra);
      // End of central directory
      out.putInt(END_SIGNATURE)
          .putShort((short) 0)
          .putShort((short) 0)
          .putShort((short) newEntries)
          .putShort((short) newEntries)
          .putInt(newDirectorySize)
          .putInt((int) newDirectoryOffset)
          .putShort((short) comment.length)
          .put(comment);
      out.flip();

      long position = dataEnd;
      while (out.hasRemaining()) {
        position += channel.write(out, position);
      }
      channel.truncate(position);
    }
  }

  /** The end of central directory record in {@code tail}, or -1. */
  private static int findEnd(ByteBuffer tail) {
    for (int i = tail.limit() - END_SIZE; i >= 0; i--) {
      if (tail.getInt(i) == END_SIGNATURE && i + END_SIZE + u16(tail, i + 20) == tail.limit()) {
        return i;
      }
    }
    return -1;
  }

  private static List<CentralRecord> parse(ByteBuffer directory, int entries, Path zip)
      throws IOException {
    List<CentralRecord> records = new ArrayList<>(entries);
    int pos = 0;
    for (int i = 0; i < entries; i++) {
      if (pos + CENTRAL_HEADER_SIZE > directory.limit()
          || directory.getInt(pos) != CENTRAL_HEADER_SIGNATURE) {
        throw new IOException(zip + ": central directory record " + i + " is damaged");
      }
      int nameLength = u16(directory, pos + 28);
      int extraLength = u16(directory, pos + 30);
      int commentLength = u16(directory, pos + 32);
      int length = CENTRAL_HEADER_SIZE + nameLength + extraLength + commentLength;
      if (pos + length > directory.limit()) {
        throw new IOException(zip + ": central directory record " + i + " is damaged");
      }
      byte[] nameBytes = new byte[nameLength];
      directory.get(pos + CENTRAL_HEADER_SIZE, nameBytes);
      records.add(
          new CentralRecord(
              pos,
              length,
              new String(nameBytes, StandardCharsets.UTF_8),
              u16(directory, pos + 4),
              u16(directory, pos + 12),
              u16(directory, pos + 14),
              u32(directory, pos + 42),
              modifiedTime(directory, pos + CENTRAL_HEADER_SIZE + nameLength, extraLength)));
      pos += length;
    }
    if (pos != directory.limit()) {
      throw new IOException(
          zip + ": the central directory holds more than " + entries + " records");
    }
    return records;
  }

  /** The modification time of an extended timestamp field, in Unix seconds, or {@code null}. */
  private static Integer modifiedTime(ByteBuffer buffer, int start, int length) {
    int pos = start;
    int end = start + length;
    while (pos + 4 <= end) {
      int id = u16(buffer, pos);
      int size = u16(buffer, pos + 2);
      if (id == EXTENDED_TIMESTAMP_ID && size >= 5 && pos + 4 + size <= end) {
        return (buffer.get(pos + 4) & 1) != 0 ? buffer.getInt(pos + 5) : null;
      }
      pos += 4 + size;
    }
    return null;
  }

  /** An extended timestamp field holding only the modification time. */
  private static byte[] extendedTimestamp(int modifiedTime) {
    return ByteBuffer.allocate(9)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putShort((short) EXTENDED_TIMESTAMP_ID)
        .putShort((short) 5)
        .put((byte) 1)
        .putInt(modifiedTime)
        .array();
  }

  private static ByteBuffer read(FileChannel channel, long position, int length)
      throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
    while (buffer.hasRemaining()) {
      if (channel.read(buffer, position + buffer.position()) < 0) {
        throw new EOFException("Unexpected end of zip at " + (position + buffer.position()));
      }
    }
    return buffer.flip();
  }

  private static int u16(ByteBuffer buffer, int index) {
    return buffer.getShort(index) & 0xFFFF;
  }

  private static long u32(ByteBuffer buffer, int index) {
    return buffer.getInt(index) & 0xFFFFFFFFL;
  }
}
