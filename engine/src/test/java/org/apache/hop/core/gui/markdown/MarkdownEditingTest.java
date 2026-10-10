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

package org.apache.hop.core.gui.markdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.gui.markdown.MarkdownEditing.Edit;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MarkdownEditingTest {

  @Test
  void boldWrapsAndUnwrapsWithDoubleUnderscores() {
    Edit wrapped = MarkdownEditing.toggleBold("say hi", 4, 6);
    assertEquals("say __hi__", apply("say hi", wrapped));
    assertEquals(10, wrapped.caret());

    Edit unwrapped = MarkdownEditing.toggleBold("say __hi__", 4, 10);
    assertEquals("say hi", apply("say __hi__", unwrapped));
  }

  @Test
  void italicUsesASingleUnderscoreAndLeavesBoldAlone() {
    Edit wrapped = MarkdownEditing.toggleItalic("say hi", 4, 6);
    assertEquals("say _hi_", apply("say hi", wrapped));

    Edit unwrapped = MarkdownEditing.toggleItalic("say _hi_", 4, 8);
    assertEquals("say hi", apply("say _hi_", unwrapped));

    Edit boldStaysBold = MarkdownEditing.toggleItalic("__hi__", 0, 6);
    assertEquals("___hi___", apply("__hi__", boldStaysBold));
  }

  @Test
  void codeBlockInsertsFencesAndRemovesThem() {
    Edit empty = MarkdownEditing.codeBlock("ab", 1, 1);
    assertEquals("a```\n\n```\nb", apply("ab", empty));
    assertEquals(5, empty.caret());

    Edit wrapped = MarkdownEditing.codeBlock("int a;", 0, 6);
    assertEquals("```\nint a;\n```\n", wrapped.replacement());

    String fenced = "```\nint a;\n```\n";
    Edit unwrapped = MarkdownEditing.codeBlock(fenced, 0, fenced.length());
    assertEquals("int a;", unwrapped.replacement());
  }

  @Test
  void headerRewritesTheTouchedLines() {
    Edit second = MarkdownEditing.applyHeader("a\nb\nc", 2, 2, 1);
    assertEquals("a\n# b\nc", apply("a\nb\nc", second));

    Edit both = MarkdownEditing.applyHeader("a\nb", 0, 3, 3);
    assertEquals("### a\n### b", both.replacement());

    Edit demoted = MarkdownEditing.applyHeader("## Title", 0, 8, 1);
    assertEquals("# Title", demoted.replacement());

    Edit plain = MarkdownEditing.applyHeader("#### Title", 0, 10, 0);
    assertEquals("Title", plain.replacement());

    Edit keepsNextLine = MarkdownEditing.applyHeader("hello\nworld", 0, 6, 2);
    assertEquals("## hello\nworld", apply("hello\nworld", keepsNextLine));
  }

  @Test
  void linkAndImageEscapeLabelAndDestination() {
    assertEquals(
        "[hop](https://hop.apache.org)", MarkdownEditing.link("hop", "https://hop.apache.org"));
    assertEquals(
        "[https://hop.apache.org](https://hop.apache.org)",
        MarkdownEditing.link("", "https://hop.apache.org"));
    assertEquals("[a\\]b](https://x)", MarkdownEditing.link("a]b", "https://x"));
    assertEquals("[file](<my file.png>)", MarkdownEditing.link("file", "my file.png"));
    assertEquals("[file](<file(1).png>)", MarkdownEditing.link("file", "file(1).png"));
    assertEquals("![a.png](images/a.png)", MarkdownEditing.image("", "images/a.png"));
    assertEquals("![diagram](<my pic.png>)", MarkdownEditing.image("diagram", "my pic.png"));
  }

  @Test
  void tableIncludesHeaderSeparatorAndBody() {
    String left = MarkdownEditing.table(2, 3, MarkdownTableAlignment.LEFT);
    assertEquals("|  |  |\n| :--- | :--- |\n|  |  |\n|  |  |\n", left);

    String center = MarkdownEditing.table(1, 2, MarkdownTableAlignment.CENTER);
    assertEquals("|  |\n| :---: |\n|  |\n", center);

    String right = MarkdownEditing.table(1, 1, MarkdownTableAlignment.RIGHT);
    assertEquals("|  |\n| ---: |\n|  |\n", right);

    assertEquals("|  |\n| --- |\n|  |\n", MarkdownEditing.table(0, 0, null));
    assertEquals("DEFAULT", MarkdownTableAlignment.DEFAULT.getCode());
    assertTrue(MarkdownTableAlignment.LEFT.getDescription().contains(":---"));
    assertFalse(MarkdownTableAlignment.getDescriptions().length < 4);
  }

  @Test
  void relativePathUsesTheBaseFileParent(@TempDir Path dir) throws Exception {
    Path notes = dir.resolve("notes");
    Files.createDirectories(notes.resolve("images"));
    Path base = notes.resolve("pipeline.hpl");
    Path image = notes.resolve("images").resolve("my pic.png");
    Path sibling = notes.resolve("other.hpl");
    Files.writeString(base, "<pipeline/>");
    Files.writeString(image, "png");
    Files.writeString(sibling, "<pipeline/>");

    Variables variables = new Variables();
    assertEquals(
        "images/my pic.png",
        MarkdownEditing.toMarkdownPath(
            variables, base.toAbsolutePath().toString(), image.toAbsolutePath().toString()));
    assertEquals(
        "other.hpl",
        MarkdownEditing.toMarkdownPath(
            variables, base.toAbsolutePath().toString(), sibling.toAbsolutePath().toString()));

    String absolute = image.toAbsolutePath().toString();
    assertEquals(absolute, MarkdownEditing.toMarkdownPath(variables, null, absolute));
    assertEquals(absolute, MarkdownEditing.toMarkdownPath(variables, "", absolute));
  }

  private static String apply(String text, Edit edit) {
    return text.substring(0, edit.start()) + edit.replacement() + text.substring(edit.end());
  }
}
