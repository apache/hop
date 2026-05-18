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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

public final class TextDiff {

  private static final int MAX_CHAIN_LENGTH = 64;

  @Getter @Setter private boolean ignoreWhiteSpaces = false;

  public TextDiff() {}

  public List<Diff> diff(String leftText, String rightText) {
    List<Line> leftLines = splitLines(leftText);
    List<Line> rightLines = splitLines(rightText);

    return findDiffs(
        new ArrayList<>(), leftLines, 0, leftLines.size(), rightLines, 0, rightLines.size());
  }

  private List<Diff> findDiffs(
      List<Diff> diffs,
      List<Line> leftLines,
      int leftLineStart,
      int leftLineEnd,
      List<Line> rightLines,
      int rightLineStart,
      int rightLineEnd) {

    while (leftLineStart < leftLineEnd && rightLineStart < rightLineEnd) {
      Anchor anchor =
          findLongestCommonAnchor(
              leftLines, leftLineStart, leftLineEnd, rightLines, rightLineStart, rightLineEnd);

      if (anchor == null) {
        addDiff(
            diffs, leftLines, leftLineStart, leftLineEnd, rightLines, rightLineStart, rightLineEnd);
        return diffs;
      }

      if (anchor.leftIndex > leftLineStart || anchor.rightIndex > rightLineStart) {
        findDiffs(
            diffs,
            leftLines,
            leftLineStart,
            anchor.leftIndex,
            rightLines,
            rightLineStart,
            anchor.rightIndex);
      }

      // Here both sides are equal

      leftLineStart = anchor.leftIndex + 1;
      rightLineStart = anchor.rightIndex + 1;
    }

    if (leftLineStart < leftLineEnd || rightLineStart < rightLineEnd) {
      addDiff(
          diffs, leftLines, leftLineStart, leftLineEnd, rightLines, rightLineStart, rightLineEnd);
    }

    return diffs;
  }

  private Anchor findLongestCommonAnchor(
      List<Line> leftLines,
      int leftLineStart,
      int leftLineEnd,
      List<Line> rightLines,
      int rightLineStart,
      int rightLineEnd) {

    // Forward direction: left to right
    Map<Integer, List<Integer>> leftBuckets = new HashMap<>();

    for (int i = leftLineStart; i < leftLineEnd; i++) {
      Line line = leftLines.get(i);
      List<Integer> positions = leftBuckets.computeIfAbsent(line.hash, k -> new ArrayList<>());
      positions.add(i);
    }

    Anchor bestForward = null;

    for (int j = rightLineStart; j < rightLineEnd; j++) {
      Line line = rightLines.get(j);
      List<Integer> positions = leftBuckets.get(line.hash);
      if (positions == null) {
        continue;
      }
      if (positions.size() > MAX_CHAIN_LENGTH) {
        continue;
      }

      for (int i : positions) {
        if (leftLines.get(i).hash != line.hash) {
          continue;
        }

        int score = positions.size();
        if (bestForward == null
            || score < bestForward.score
            || (score == bestForward.score && i < bestForward.leftIndex)
            || (score == bestForward.score
                && i == bestForward.leftIndex
                && j < bestForward.rightIndex)) {
          bestForward = new Anchor(i, j, score);
        }
      }
    }

    // Backward direction: right to left
    Map<Integer, List<Integer>> rightBuckets = new HashMap<>();

    for (int j = rightLineStart; j < rightLineEnd; j++) {
      Line line = rightLines.get(j);
      List<Integer> positions = rightBuckets.computeIfAbsent(line.hash, k -> new ArrayList<>());
      positions.add(j);
    }

    Anchor bestBackward = null;

    for (int i = leftLineStart; i < leftLineEnd; i++) {
      Line line = leftLines.get(i);
      List<Integer> positions = rightBuckets.get(line.hash);
      if (positions == null) {
        continue;
      }
      if (positions.size() > MAX_CHAIN_LENGTH) {
        continue;
      }

      for (int j : positions) {
        if (rightLines.get(j).hash != line.hash) {
          continue;
        }

        int score = positions.size();
        if (bestBackward == null
            || score < bestBackward.score
            || (score == bestBackward.score && i < bestBackward.leftIndex)
            || (score == bestBackward.score
                && i == bestBackward.leftIndex
                && j < bestBackward.rightIndex)) {
          bestBackward = new Anchor(i, j, score);
        }
      }
    }

    // Select the best anchor from both directions
    if (bestForward == null) {
      return bestBackward;
    }
    if (bestBackward == null) {
      return bestForward;
    }

    // Prefer the anchor with lower score, or earlier position if scores are equal
    if (bestForward.score < bestBackward.score
        || (bestForward.score == bestBackward.score
            && bestForward.leftIndex < bestBackward.leftIndex)
        || (bestForward.score == bestBackward.score
            && bestForward.leftIndex == bestBackward.leftIndex
            && bestForward.rightIndex < bestBackward.rightIndex)) {
      return bestForward;
    }

    return bestBackward;
  }

  private void addDiff(
      List<Diff> diffs,
      List<Line> leftLines,
      int leftLineStart,
      int leftLineEnd,
      List<Line> rightLines,
      int rightLineStart,
      int rightLineEnd) {

    if (leftLineStart < leftLineEnd && rightLineStart < rightLineEnd) {
      Line leftStart = leftLines.get(leftLineStart);
      Line leftEnd = leftLines.get(leftLineEnd - 1);
      Line rightStart = rightLines.get(rightLineStart);
      Line rightEnd = rightLines.get(rightLineEnd - 1);

      Diff diff =
          new Diff(
              Diff.Type.CHANGE,
              leftLineStart,
              leftLineEnd - leftLineStart,
              leftStart.start,
              leftEnd.end - leftStart.start,
              rightLineStart,
              rightLineEnd - rightLineStart,
              rightStart.start,
              rightEnd.end - rightStart.start);
      diffs.add(diff);
      return;
    }

    if (leftLineStart < leftLineEnd) {
      Line leftStart = leftLines.get(leftLineStart);
      Line leftEnd = leftLines.get(leftLineEnd - 1);
      Diff diff =
          new Diff(
              Diff.Type.DELETE,
              leftLineStart,
              leftLineEnd - leftLineStart,
              leftStart.start,
              leftEnd.end - leftStart.start,
              rightLineStart,
              0,
              -1,
              -1);
      diffs.add(diff);
      return;
    }

    if (rightLineStart < rightLineEnd) {
      Line rightStart = rightLines.get(rightLineStart);
      Line rightEnd = rightLines.get(rightLineEnd - 1);
      Diff diff =
          new Diff(
              Diff.Type.INSERT,
              leftLineStart,
              0,
              -1,
              -1,
              rightLineStart,
              rightLineEnd - rightLineStart,
              rightStart.start,
              rightEnd.end - rightStart.start);
      diffs.add(diff);
    }
  }

  /**
   * Splits text into lines. Line comparison ignores leading and trailing whitespace, but preserves
   * original positions and text content.
   */
  private List<Line> splitLines(String text) {
    if (text == null || text.isEmpty()) {
      return List.of();
    }

    List<Line> lines = new ArrayList<>();

    int length = text.length();
    int row = 0;
    int start = 0;

    for (int i = 0; i < length; i++) {
      char c = text.charAt(i);
      if (c == '\n' || c == '\r') {
        // Handle \r\n as single line break
        if (c == '\r' && i + 1 < length && text.charAt(i + 1) == '\n') {
          String str = text.substring(start, i);
          lines.add(new Line(str, row++, start, i, computeHash(str)));
          start = i + 2;
          i++; // Skip the \n
        } else {
          String str = text.substring(start, i);
          lines.add(new Line(str, row++, start, i, computeHash(str)));
          start = i + 1;
        }
      }
    }

    // Add final line if text doesn't end with newline
    if (start <= length) {
      String str = text.substring(start);
      lines.add(new Line(str, row, start, length, computeHash(str)));
    }

    return lines;
  }

  private int computeHash(String str) {
    if (ignoreWhiteSpaces) {
      str = StringUtils.normalizeSpace(str);
    }
    return str.hashCode();
  }

  private record Anchor(int leftIndex, int rightIndex, int score) {}

  private record Line(String text, int row, int start, int end, int hash) {}
}
