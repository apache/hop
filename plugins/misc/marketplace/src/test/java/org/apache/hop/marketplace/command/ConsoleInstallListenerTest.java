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

package org.apache.hop.marketplace.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.marketplace.install.IInstallListener;
import org.junit.jupiter.api.Test;

/**
 * The CLI progress bar: layout, throttling, in-place redraw, and the redirected-output fallback.
 */
class ConsoleInstallListenerTest {

  private static final char[] ASCII = {'#', '-'};

  /** Manually advanced clock so throttling is deterministic. */
  private static class FakeClock implements ConsoleInstallListener.IClock {
    private long now;

    @Override
    public long millis() {
      return now;
    }
  }

  private static class Capture {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final PrintStream stream = new PrintStream(bytes, true, StandardCharsets.UTF_8);

    private String text() {
      return bytes.toString(StandardCharsets.UTF_8);
    }
  }

  @Test
  void drawsABarWithPercentSpeedAndEta() {
    Capture capture = new Capture();
    FakeClock clock = new FakeClock();
    ConsoleInstallListener listener =
        new ConsoleInstallListener(capture.stream, true, ASCII, clock::millis);

    listener.started("Data Vault", 353L * 1024 * 1024);
    // 42MB in 24s ≈ 1.75MB/s, leaving 311MB ≈ 178s ≈ 3 mins.
    clock.now += 24_000;
    listener.transferred(42L * 1024 * 1024, 353L * 1024 * 1024);
    listener.complete();

    String output = capture.text();
    assertTrue(
        output.startsWith("Downloading: Data Vault (353MB)"),
        "size should be announced up front: " + output);
    assertTrue(
        output.contains("[##----------------------]  11%  1.8MB/s - 42.0MB of 353MB, 3 mins left"),
        "unexpected bar line: " + output);
  }

  @Test
  void barFillsProportionallyAndCompletely() {
    Capture capture = new Capture();
    FakeClock clock = new FakeClock();
    ConsoleInstallListener listener =
        new ConsoleInstallListener(capture.stream, true, ASCII, clock::millis);
    listener.started("plugin", 100L);
    clock.now += 1000;

    assertTrue(listener.progressLine(50L, 100L).contains("[############------------]  50%"));
    assertTrue(
        listener.progressLine(100L, 100L).contains("[########################] 100%"),
        "a finished transfer should show a full bar");
    assertTrue(
        listener.progressLine(0L, 100L).contains("[------------------------]   0%"),
        "an empty bar at the start");
  }

  @Test
  void redrawErasesTheTailOfALongerPreviousLine() {
    Capture capture = new Capture();
    FakeClock clock = new FakeClock();
    ConsoleInstallListener listener =
        new ConsoleInstallListener(capture.stream, true, ASCII, clock::millis);

    listener.started("plugin", 1000L);
    clock.now += 1000;
    listener.transferred(500L, 1000L);
    int firstLineLength = lastCarriageReturnSegment(capture.text()).length();
    // A shorter follow-up line (no ETA once the transfer is complete) must not leave debris behind.
    clock.now += 1000;
    listener.transferred(1000L, 1000L);

    String tail = lastCarriageReturnSegment(capture.text());
    assertTrue(
        tail.length() >= firstLineLength,
        "the shorter line must be padded out to erase the longer one it replaced");
    assertTrue(tail.endsWith(" ") || tail.contains("100%"), "unexpected redraw: [" + tail + "]");
  }

  @Test
  void throttlesRedrawsRegardlessOfChunkCount() {
    Capture capture = new Capture();
    FakeClock clock = new FakeClock();
    ConsoleInstallListener listener =
        new ConsoleInstallListener(capture.stream, true, ASCII, clock::millis);
    listener.started("plugin", 10_000_000L);

    for (int i = 1; i <= 2000; i++) {
      clock.now += 1; // 1ms apart, i.e. 2s of transfer
      listener.transferred(i * 5000L, 10_000_000L);
    }

    long frames = capture.text().chars().filter(c -> c == '\r').count();
    // 2000ms at a 200ms floor allows ~10 frames. Near 2000 would mean the throttle is broken.
    assertTrue(frames <= 15, "expected the throttle to collapse 2000 chunks, got " + frames);
    assertTrue(frames >= 3, "throttle should still produce visible movement, got " + frames);
  }

  @Test
  void redirectedOutputGetsMilestonesButNoBarFrames() {
    Capture capture = new Capture();
    FakeClock clock = new FakeClock();
    // interactive=false: stdout is a file or a pipe.
    ConsoleInstallListener listener =
        new ConsoleInstallListener(capture.stream, false, ASCII, clock::millis);

    listener.started("Data Vault", 1000L);
    for (int i = 1; i <= 100; i++) {
      clock.now += 500;
      listener.transferred(i * 10L, 1000L);
    }
    listener.phase(IInstallListener.Phase.UNZIP, "hop-datavault");
    listener.complete();

    String output = capture.text();
    assertFalse(output.contains("\r"), "a redirected log must not be filled with bar frames");
    assertTrue(output.contains("Downloading: Data Vault (1000B)"), output);
    assertTrue(output.contains("Unpacking..."), output);
  }

  @Test
  void batchItemsArePrefixedWithTheirPosition() {
    Capture capture = new Capture();
    ConsoleInstallListener listener =
        new ConsoleInstallListener(capture.stream, true, ASCII, new FakeClock()::millis);

    listener.item("hop-tech-parquet", 2, 12);
    assertTrue(capture.text().contains("[3/12] hop-tech-parquet"), capture.text());
  }

  @Test
  void singleInstallHasNoBatchPrefix() {
    Capture capture = new Capture();
    ConsoleInstallListener listener =
        new ConsoleInstallListener(capture.stream, true, ASCII, new FakeClock()::millis);

    listener.item("hop-datavault", 0, 1);
    assertEquals("", capture.text(), "a lone artifact should not be numbered '1/1'");
  }

  @Test
  void unknownSizeDropsTheBarAndTheEta() {
    Capture capture = new Capture();
    FakeClock clock = new FakeClock();
    ConsoleInstallListener listener =
        new ConsoleInstallListener(capture.stream, true, ASCII, clock::millis);

    listener.started("plugin", -1L);
    clock.now += 1000;
    listener.transferred(500_000L, -1L);

    String line = lastCarriageReturnSegment(capture.text());
    assertFalse(line.contains("["), "no total means no meaningful bar: " + line);
    assertFalse(line.contains("left"), "no total means no honest ETA: " + line);
    assertTrue(line.contains("488KB downloaded"), line);
  }

  @Test
  void asciiGlyphsAreUsedWhenTheEncodingCannotCarryBlocks() {
    PrintStream ascii =
        new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.US_ASCII);
    PrintStream utf8 = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);

    assertEquals('#', ConsoleInstallListener.glyphsFor(ascii)[0], "US-ASCII cannot encode a block");
    assertEquals('█', ConsoleInstallListener.glyphsFor(utf8)[0], "UTF-8 can");
  }

  /** Everything written after the final carriage return, i.e. the currently visible line. */
  private static String lastCarriageReturnSegment(String output) {
    int index = output.lastIndexOf('\r');
    return index < 0 ? output : output.substring(index + 1);
  }
}
