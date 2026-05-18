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
 *
 */

package org.apache.hop.git.diff;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class TextDiffTest {

  static List<Diff> diff(String left, String right) {
    TextDiff textDiff = new TextDiff();
    List<Diff> diffs = textDiff.diff(left, right);

    diffs.forEach(
        diff -> {
          System.out.println(diff.toString());
        });

    return diffs;
  }

  @Test
  void testMultiLineChar() {
    String left = "ligne 1\nligne 2 AAA\nligne 3";
    String right = "ligne 1\nligne 2 BBB\nligne 3";
    List<Diff> diffs = diff(left, right);
    assertEquals(1, diffs.size());
  }

  @Test
  void testMultiLines() {
    String left = "ligne 1\nligne 2\nligne 3\nligne 4";
    String right = "ligne 11\nligne 2\nligne\nligne modified\nligne 3";
    List<Diff> diffs = diff(left, right);
    assertEquals(3, diffs.size());
  }

  @Test
  void testEmptyLeft() {
    List<Diff> diffs = diff("", "Test");
    assertEquals(1, diffs.size());

    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.INSERT, diff.type());
    assertEquals(0, diff.leftLineStart());
    assertEquals(0, diff.leftLineCount());
    assertEquals(0, diff.rightLineStart());
    assertEquals(1, diff.rightLineCount());
  }

  @Test
  void testEmptyRight() {
    List<Diff> diffs = diff("Test", "");
    assertEquals(1, diffs.size());

    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.DELETE, diff.type());
    assertEquals(0, diff.leftLineStart());
    assertEquals(1, diff.leftLineCount());
    assertEquals(0, diff.rightLineStart());
    assertEquals(0, diff.rightLineCount());
  }

  @Test
  void testNullRight() {
    List<Diff> diffs = diff("Test", null);
    assertEquals(1, diffs.size());

    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.DELETE, diff.type());
    assertEquals(0, diff.leftLineStart());
    assertEquals(1, diff.leftLineCount());
    assertEquals(0, diff.rightLineStart());
    assertEquals(0, diff.rightLineCount());
  }

  @Test
  void testNullLeft() {
    List<Diff> diffs = diff(null, "Test");
    assertEquals(1, diffs.size());

    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.INSERT, diff.type());
    assertEquals(0, diff.leftLineStart());
    assertEquals(0, diff.leftLineCount());
    assertEquals(0, diff.rightLineStart());
    assertEquals(1, diff.rightLineCount());
  }

  @Test
  void testChangeTop() {
    List<Diff> diffs = diff("AAA\nBBB", "XX\nBBB");

    assertEquals(1, diffs.size());
    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.CHANGE, diff.type());
    assertEquals(0, diff.leftLineStart());
    assertEquals(1, diff.leftLineCount());
    assertEquals(0, diff.leftStart());
    assertEquals(3, diff.leftCount());
    assertEquals(0, diff.rightLineStart());
    assertEquals(1, diff.rightLineCount());
    assertEquals(0, diff.rightStart());
    assertEquals(2, diff.rightCount());
  }

  @Test
  void testChangeMiddle() {
    List<Diff> diffs = diff("AAA\nBBB\nCCC", "AAA\nXX\nCCC");
    assertEquals(1, diffs.size());

    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.CHANGE, diff.type());
    assertEquals(1, diff.leftLineStart());
    assertEquals(1, diff.leftLineCount());
    assertEquals(4, diff.leftStart());
    assertEquals(3, diff.leftCount());
    assertEquals(1, diff.rightLineStart());
    assertEquals(1, diff.rightLineCount());
    assertEquals(4, diff.rightStart());
    assertEquals(2, diff.rightCount());
  }

  @Test
  void testChangeMulti() {
    List<Diff> diffs = diff("AAA\n\n\nBBB\nCCC\n", "AAA\n\n\nXX\nCCC\nDDD");
    assertEquals(2, diffs.size());

    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.CHANGE, diff.type());
    assertEquals(3, diff.leftLineStart());
    assertEquals(1, diff.leftLineCount());
    assertEquals(6, diff.leftStart());
    assertEquals(3, diff.leftCount());
    assertEquals(3, diff.rightLineStart());
    assertEquals(1, diff.rightLineCount());
    assertEquals(6, diff.rightStart());
    assertEquals(2, diff.rightCount());

    diff = diffs.get(1);
    assertEquals(Diff.Type.CHANGE, diff.type());
    assertEquals(5, diff.leftLineStart());
    assertEquals(1, diff.leftLineCount());
    assertEquals(14, diff.leftStart());
    assertEquals(0, diff.leftCount());
    assertEquals(5, diff.rightLineStart());
    assertEquals(1, diff.rightLineCount());
    assertEquals(13, diff.rightStart());
    assertEquals(3, diff.rightCount());
  }

  @Test
  void testDeleteEnd() {
    List<Diff> diffs = diff("AAA\nBBB\nCCC", "AAA");
    assertEquals(1, diffs.size());

    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.DELETE, diff.type());
    assertEquals(1, diff.leftLineStart());
    assertEquals(2, diff.leftLineCount());
    assertEquals(1, diff.rightLineStart());
    assertEquals(0, diff.rightLineCount());
  }

  @Test
  void testDeleteTop() {
    List<Diff> diffs = diff("AAA\nAAA\nAAA\nAAA\nAAA\nAAA\nBBB\nCCC", "BBB\nCCC");
    assertEquals(1, diffs.size());

    Diff diff = diffs.getFirst();
    assertEquals(Diff.Type.DELETE, diff.type());
    assertEquals(0, diff.leftLineStart());
    assertEquals(6, diff.leftLineCount());
    assertEquals(0, diff.rightLineStart());
    assertEquals(0, diff.rightLineCount());
  }

  @Test
  void testNormalizeCarriageReturn() {
    List<Diff> diffs = diff("AAA\r\nBBB\nCCC\n", "AAA\nBBB\r\nCCC\r\n");
    assertEquals(0, diffs.size());
  }

  @Test
  void testIgnoreWhiteSpaces() {
    TextDiff textDiff = new TextDiff();
    textDiff.setIgnoreWhiteSpaces(true);
    List<Diff> diffs = textDiff.diff("\tAAA \nBBB BBB\n\t\tCCC ", "AAA\n BBB  BBB \nCCC");
    assertEquals(0, diffs.size());
  }
}
