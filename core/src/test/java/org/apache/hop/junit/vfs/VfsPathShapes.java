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
package org.apache.hop.junit.vfs;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.vfs2.FileName;
import org.junit.jupiter.api.function.Executable;

/**
 * One shared set of awkward path shapes to run through every VFS file name parser, so what a parser
 * makes of them is written down instead of assumed.
 *
 * <p>A file name parser is a handful of lines of its own on top of a Commons VFS base class, so
 * line coverage says nothing useful about one: {@code SftpFileNameParser} sits at 100% and still
 * cannot address a mainframe data set. What matters is which path shapes survive it, which is what
 * this pins down. Expectations are written as a table, one shape per line, name and expected value
 * separated by two or more spaces:
 *
 * <pre>
 *   VfsPathShapes.assertPaths("sftp://user@host:22", manager::resolveURI, """
 *       plain                   /dir/file.txt
 *       leading-double-slash    /dir/file.txt     # collapsed, see issue
 *       ...
 *       """);
 * </pre>
 *
 * <p>Every shape in {@link #SHAPES} has to appear in the table and vice versa, so a shape added
 * here surfaces as a failure in each parser's test rather than quietly going unchecked.
 *
 * @see #assertUriRoundTrips(String, NameParser) the invariant a fix has to keep
 */
public final class VfsPathShapes {

  /** Expected value for a shape the parser rejects. */
  public static final String ERROR = "error";

  /** A path shape: a name to refer to it by, and the path to append to a base URI. */
  public record Shape(String name, String suffix) {}

  /**
   * The shapes. Appended to a base URI such as {@code sftp://user@host:22} or {@code file://}, so
   * each one starts at the root of the file system.
   */
  public static final List<Shape> SHAPES =
      List.of(
          new Shape("plain", "/dir/file.txt"),
          // A mainframe data set and a Windows UNC share both live or die by these two.
          new Shape("leading-double-slash", "//dir/file.txt"),
          new Shape("leading-triple-slash", "///dir/file.txt"),
          // What a base folder ending in "/" plus a child starting with "/" concatenates to.
          new Shape("interior-double-slash", "/dir//file.txt"),
          new Shape("trailing-slash", "/dir/"),
          new Shape("root-only", "/"),
          new Shape("no-path", ""),
          new Shape("dot-segment", "/dir/./file.txt"),
          new Shape("parent-segment", "/dir/sub/../file.txt"),
          new Shape("parent-above-root", "/../file.txt"),
          new Shape("encoded-slash", "/dir/%2Ffile.txt"),
          new Shape("space", "/dir/my file.txt"),
          new Shape("encoded-space", "/dir/my%20file.txt"),
          new Shape("hash", "/dir/file#1.txt"),
          new Shape("question-mark", "/dir/file?1.txt"),
          new Shape("plus", "/dir/file+1.txt"),
          new Shape("backslash", "/dir\\file.txt"),
          new Shape("leading-dots-in-name", "/dir/..hidden"),
          new Shape("unicode", "/dir/naïve.txt"));

  /** Parses a URI into a file name, the way the provider under test would. */
  @FunctionalInterface
  public interface NameParser {
    FileName parse(String uri) throws Exception;
  }

  /**
   * Run every shape through the parser.
   *
   * @return shape name to the resulting path, or {@link #ERROR} if the parser rejected it
   */
  public static Map<String, String> paths(String baseUri, NameParser parser) {
    return collect(baseUri, parser, FileName::getPath);
  }

  /**
   * Run every shape through the parser.
   *
   * @return shape name to the resulting URI, or {@link #ERROR} if the parser rejected it
   */
  public static Map<String, String> uris(String baseUri, NameParser parser) {
    return collect(baseUri, parser, FileName::getURI);
  }

  private static Map<String, String> collect(
      String baseUri, NameParser parser, Function<FileName, String> render) {
    Map<String, String> actual = new LinkedHashMap<>();
    for (Shape shape : SHAPES) {
      try {
        actual.put(shape.name(), render.apply(parser.parse(baseUri + shape.suffix())));
      } catch (Exception e) {
        actual.put(shape.name(), ERROR);
      }
    }
    return actual;
  }

  /** Assert the path each shape parses to, reporting every shape which moved at once. */
  public static void assertPaths(String baseUri, NameParser parser, String expectedTable) {
    assertEvery(paths(baseUri, parser), expectedTable, "path");
  }

  /** Assert the URI each shape parses to, reporting every shape which moved at once. */
  public static void assertUris(String baseUri, NameParser parser, String expectedTable) {
    assertEvery(uris(baseUri, parser), expectedTable, "URI");
  }

  /**
   * One shape per line: the shape name, two or more spaces, the expected value. Blank lines are
   * ignored, and so is a trailing note introduced by a {@code #} which is itself preceded by two or
   * more spaces - the same separator the columns use, so that a {@code #} inside an expected value
   * stays part of the value.
   */
  private static Map<String, String> parseTable(String table) {
    Map<String, String> expected = new LinkedHashMap<>();
    for (String rawLine : table.split("\n")) {
      String line = rawLine.replaceAll("\\s{2,}#.*$", "").strip();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      String[] cells = line.split("\\s{2,}", 2);
      if (cells.length != 2) {
        fail(
            "table line needs a name and a value separated by two or more spaces: \""
                + line
                + "\"");
      }
      expected.put(cells[0].strip(), cells[1].strip());
    }
    return expected;
  }

  private static void assertEvery(Map<String, String> actual, String table, String what) {
    Map<String, String> expected = parseTable(table);

    List<String> unknown = new ArrayList<>(expected.keySet());
    unknown.removeAll(actual.keySet());
    if (!unknown.isEmpty()) {
      fail("table has rows for shapes which do not exist: " + unknown);
    }

    List<Executable> checks = new ArrayList<>();
    for (Shape shape : SHAPES) {
      String name = shape.name();
      if (!expected.containsKey(name)) {
        checks.add(
            () ->
                fail(
                    "no expected "
                        + what
                        + " for shape \""
                        + name
                        + "\" ("
                        + shape.suffix()
                        + "), which parses to: "
                        + actual.get(name)));
        continue;
      }
      checks.add(
          () ->
              assertEquals(
                  expected.get(name),
                  actual.get(name),
                  "shape \"" + name + "\" (" + shape.suffix() + ")"));
    }
    assertAll(what + " per shape", checks);
  }

  /**
   * Assert that every shape which parses survives a trip through its own URI unchanged, for the
   * name itself and for every one of its parents.
   *
   * <p>This is the invariant a fix has to keep, and the one most easily lost. A file name carries
   * its root and its path separately, and only the root is handed on untouched: {@code
   * createName(absPath, type)} is called with an already normalised path on every child listing and
   * every {@link FileName#getParent()}. So anything a parser defends inside the <em>path</em> is
   * re-normalised away the moment a folder is listed or a parent is walked to, while anything it
   * puts in the <em>root</em> survives - which is why the Windows parser hoists a UNC share into
   * the root rather than guarding it in the path.
   */
  public static void assertUriRoundTrips(String baseUri, NameParser parser) {
    List<Executable> checks = new ArrayList<>();
    for (Shape shape : SHAPES) {
      FileName name;
      try {
        name = parser.parse(baseUri + shape.suffix());
      } catch (Exception e) {
        continue; // a shape the parser rejects has nothing to round trip
      }
      for (FileName each = name; each != null; each = each.getParent()) {
        FileName toCheck = each;
        String role = each == name ? "" : " (parent of " + name.getURI() + ")";
        checks.add(
            () -> {
              FileName reparsed = parser.parse(toCheck.getURI());
              assertEquals(
                  toCheck.getPath(),
                  reparsed.getPath(),
                  "path of \"" + shape.name() + "\"" + role + " changed on reparsing its URI");
              assertEquals(
                  toCheck.getURI(),
                  reparsed.getURI(),
                  "URI of \"" + shape.name() + "\"" + role + " changed on reparsing itself");
            });
      }
    }
    assertAll("URI round trip per shape", checks);
  }

  private VfsPathShapes() {}
}
