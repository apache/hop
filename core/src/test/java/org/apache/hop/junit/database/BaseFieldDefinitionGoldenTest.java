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
package org.apache.hop.junit.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Snapshots the DDL a database dialect generates for a fixed matrix of Hop value types, so that
 * refactoring the type mapping cannot silently change generated DDL.
 *
 * <p>The golden files record what Hop does <em>today</em>, bugs included. They are a change
 * detector, not a statement of correctness: a line that looks wrong (see the unsized INTEGER rows,
 * issue #4174) is the bug held still so that fixing it shows up as a reviewable diff.
 *
 * <p>To regenerate after an intentional change, run with {@code -Dhop.golden.update=true} and read
 * the resulting diff line by line.
 *
 * <p>Subclasses live in the database plugin they cover and only supply the dialect:
 *
 * <pre>
 * class PostgreSqlFieldDefinitionGoldenTest extends BaseFieldDefinitionGoldenTest {
 *   &#64;Override
 *   protected IDatabase createDatabase() {
 *     return new PostgreSqlDatabaseMeta();
 *   }
 * }
 * </pre>
 */
public abstract class BaseFieldDefinitionGoldenTest {

  private static final String UPDATE_PROPERTY = "hop.golden.update";

  /** The column name used throughout the matrix. */
  private static final String COLUMN = "COL";

  /** The table name used by the ALTER TABLE matrix. */
  private static final String TABLE = "TBL";

  /** Length/precision pairs exercised for every type. */
  private static final int[][] SIZES = {
    {-1, -1},
    {0, 0},
    {4, 0},
    {9, 0},
    {15, 0},
    {18, 0},
    {20, 0},
    {10, 2},
    {DatabaseMeta.CLOB_LENGTH, 0}
  };

  /** A shorter matrix for the ALTER statements: unsized, sized, and sized with a precision. */
  private static final int[][] ALTER_SIZES = {
    {-1, -1},
    {15, 0},
    {10, 2}
  };

  /**
   * @return a fresh dialect instance to snapshot.
   */
  protected abstract IDatabase createDatabase();

  /**
   * @return the classpath resource holding the golden output. Defaults to a file named after the
   *     test class, next to it.
   */
  protected String getGoldenResource() {
    return "/" + getClass().getName().replace('.', '/') + ".txt";
  }

  @BeforeAll
  static void setUpGoldenTestClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void fieldDefinitionsMatchGolden() throws IOException {
    String actual = render();

    if (Boolean.getBoolean(UPDATE_PROPERTY)) {
      writeGolden(actual);
      return;
    }

    String expected = readGolden();
    if (expected == null) {
      fail(
          "No golden file found at "
              + getGoldenResource()
              + ". Generate it with -D"
              + UPDATE_PROPERTY
              + "=true and review every line before committing.\n\n"
              + actual);
    }
    assertEquals(
        expected.trim(),
        actual.trim(),
        "Generated DDL changed for "
            + createDatabase().getClass().getSimpleName()
            + ". If the change is intended, regenerate with -D"
            + UPDATE_PROPERTY
            + "=true and justify every differing line in the commit message.");
  }

  /** Builds the full matrix as stable, diffable text. */
  private String render() {
    IDatabase database = createDatabase();
    // Through DatabaseMeta rather than the dialect directly: that is the path the engine takes,
    // so it is the only one that records what a user's database actually receives. Asking the
    // dialect on its own skips the dialect's type rules and the value types, which is how JSON
    // and INET came to be recorded as UNKNOWN everywhere.
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(database);
    StringBuilder out = new StringBuilder();
    out.append("# ").append(database.getClass().getName()).append('\n');
    out.append("# Generated by ")
        .append(getClass().getSimpleName())
        .append(". Do not hand-edit.\n");
    out.append("# Records current behaviour including known bugs. See the class javadoc.\n");

    out.append("\n[plain]\n");
    for (TypeCase type : types()) {
      for (int[] size : SIZES) {
        out.append(line(databaseMeta, type, size[0], size[1], null, null, false));
      }
    }

    // Technical/primary key and auto-increment only change the outcome for numeric types.
    out.append("\n[keys]\n");
    for (TypeCase type : numericTypes()) {
      for (int[] size : SIZES) {
        out.append(line(databaseMeta, type, size[0], size[1], COLUMN, null, false));
        out.append(line(databaseMeta, type, size[0], size[1], null, COLUMN, false));
        out.append(line(databaseMeta, type, size[0], size[1], COLUMN, null, true));
      }
    }

    // ALTER TABLE has to spell a column the way CREATE TABLE did. It reaches the type through a
    // different route, so it is snapshotted separately; a few sizes are enough, because what the
    // type resolves to is already covered above and what is under test here is the route.
    out.append("\n[alter]\n");
    for (TypeCase type : types()) {
      for (int[] size : ALTER_SIZES) {
        out.append(alterLine(databaseMeta, type, size[0], size[1], true));
        out.append(alterLine(databaseMeta, type, size[0], size[1], false));
      }
    }
    return out.toString();
  }

  /** One ADD COLUMN or MODIFY COLUMN statement, flattened to a single line. */
  private String alterLine(
      DatabaseMeta databaseMeta, TypeCase type, int length, int precision, boolean add) {

    IValueMeta v = type.create();
    v.setLength(length);
    v.setPrecision(precision);

    String result;
    try {
      String statement =
          add
              ? databaseMeta.getAddColumnStatement(TABLE, v, null, false, null, false)
              : databaseMeta.getModifyColumnStatement(TABLE, v, null, false, null, false);
      result = statement == null ? "<null>" : statement.replaceAll("\\s+", " ").trim();
    } catch (Exception e) {
      result = "!! " + e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    return String.format(
        "%-12s len=%8d prec=%4d %-8s -> %s%n",
        type.name, length, precision, add ? "add" : "modify", result);
  }

  private String line(
      DatabaseMeta databaseMeta,
      TypeCase type,
      int length,
      int precision,
      String tk,
      String pk,
      boolean useAutoIncrement) {

    // A fresh value meta every time: several dialects mutate it (CLOB_LENGTH clamping).
    IValueMeta v = type.create();
    v.setLength(length);
    v.setPrecision(precision);

    String result;
    try {
      // addFieldName=false, addCr=false so the golden holds the type only.
      result = databaseMeta.getFieldDefinition(v, tk, pk, useAutoIncrement, false, false);
      result = result == null ? "<null>" : result.trim();
    } catch (Exception e) {
      // Capture crashes too: some combinations throw today and that is worth freezing.
      result = "!! " + e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    String flag;
    if (useAutoIncrement) {
      flag = "autoinc";
    } else if (tk != null) {
      flag = "tk";
    } else if (pk != null) {
      flag = "pk";
    } else {
      flag = "-";
    }

    return String.format(
        "%-12s len=%8d prec=%4d %-8s -> %s%n", type.name, length, precision, flag, result);
  }

  private List<TypeCase> types() {
    List<TypeCase> list = new ArrayList<>();
    list.add(new TypeCase("STRING", () -> new ValueMetaString(COLUMN)));
    list.add(new TypeCase("INTEGER", () -> new ValueMetaInteger(COLUMN)));
    list.add(new TypeCase("NUMBER", () -> new ValueMetaNumber(COLUMN)));
    list.add(new TypeCase("BIGNUMBER", () -> new ValueMetaBigNumber(COLUMN)));
    list.add(new TypeCase("DATE", () -> new ValueMetaDate(COLUMN)));
    list.add(new TypeCase("TIMESTAMP", () -> new ValueMetaTimestamp(COLUMN)));
    list.add(new TypeCase("BOOLEAN", () -> new ValueMetaBoolean(COLUMN)));
    list.add(new TypeCase("BINARY", () -> new ValueMetaBinary(COLUMN)));
    list.add(new TypeCase("INET", () -> new ValueMetaInternetAddress(COLUMN)));
    list.add(new TypeCase("JSON", () -> new ValueMetaJson(COLUMN)));
    return list;
  }

  private List<TypeCase> numericTypes() {
    List<TypeCase> list = new ArrayList<>();
    list.add(new TypeCase("INTEGER", () -> new ValueMetaInteger(COLUMN)));
    list.add(new TypeCase("NUMBER", () -> new ValueMetaNumber(COLUMN)));
    list.add(new TypeCase("BIGNUMBER", () -> new ValueMetaBigNumber(COLUMN)));
    return list;
  }

  private String readGolden() throws IOException {
    try (InputStream in = getClass().getResourceAsStream(getGoldenResource())) {
      if (in == null) {
        return null;
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** Writes the golden into src/test/resources so the update run produces a reviewable diff. */
  private void writeGolden(String content) throws IOException {
    Path target =
        Paths.get("src", "test", "resources")
            .resolve(getGoldenResource().substring(1))
            .toAbsolutePath();
    Files.createDirectories(target.getParent());
    Files.writeString(target, content, StandardCharsets.UTF_8);
    System.out.println("Wrote golden file: " + target);
  }

  /** One value type in the matrix, created fresh per case. */
  private static final class TypeCase {
    private final String name;
    private final Supplier<IValueMeta> factory;

    private TypeCase(String name, Supplier<IValueMeta> factory) {
      this.name = name;
      this.factory = factory;
    }

    private IValueMeta create() {
      return factory.get();
    }
  }
}
