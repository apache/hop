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

package org.apache.hop.core.row;

import java.util.Date;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;

public class SpeedTest {
  private static final String CONST_TIMES = " times : ";
  private static final String CONST_MS = " ms (";
  private static final String CONST_RS = " r/s)";
  private static final String CONST_STRINGS = "String";
  private final Object[] rowString10;
  private final Object[] rowString100;
  private final Object[] rowString1000;

  private final Object[] rowMixed10;
  private final Object[] rowMixed100;
  private final Object[] rowMixed1000;

  private final IRowMeta metaString10;
  private final IRowMeta metaMixed10;

  private final IRowMeta metaString100;
  private final IRowMeta metaMixed100;

  private final IRowMeta metaString1000;
  private final IRowMeta metaMixed1000;

  public SpeedTest() {
    rowString10 = new Object[10];
    rowString100 = new Object[100];
    rowString1000 = new Object[1000];

    rowMixed10 = new Object[50];
    rowMixed100 = new Object[500];
    rowMixed1000 = new Object[5000];

    metaString10 = new RowMeta();
    metaMixed10 = new RowMeta();

    metaString100 = new RowMeta();
    metaMixed100 = new RowMeta();

    metaString1000 = new RowMeta();
    metaMixed1000 = new RowMeta();

    for (int i = 0; i < 10; i++) {
      populateMetaAndData(i, rowString10, metaString10, rowMixed10, metaMixed10);
    }

    for (int i = 0; i < 100; i++) {
      populateMetaAndData(i, rowString100, metaString100, rowMixed100, metaMixed100);
    }

    for (int i = 0; i < 1000; i++) {
      populateMetaAndData(i, rowString1000, metaString1000, rowMixed1000, metaMixed1000);
    }
  }

  private static void populateMetaAndData(
      int i,
      Object[] rowString10,
      IRowMeta metaString10,
      Object[] rowMixed10,
      IRowMeta metaMixed10) {
    rowString10[i] = StringUtil.generateRandomString(20, "", "", false);
    IValueMeta meta = new ValueMetaString(CONST_STRINGS + (i + 1), 20, 0);
    metaString10.addValueMeta(meta);

    rowMixed10[i * 5] = StringUtil.generateRandomString(20, "", "", false);
    IValueMeta meta0 = new ValueMetaString(CONST_STRINGS + (i * 5 + 1), 20, 0);
    metaMixed10.addValueMeta(meta0);

    rowMixed10[i * 5 + 1] = new Date();
    IValueMeta meta1 = new ValueMetaDate(CONST_STRINGS + (i * 5 + 1));
    metaMixed10.addValueMeta(meta1);

    rowMixed10[i * 5 + 2] = Double.valueOf(Math.random() * 1000000);
    IValueMeta meta2 = new ValueMetaNumber(CONST_STRINGS + (i * 5 + 1), 12, 4);
    metaMixed10.addValueMeta(meta2);

    rowMixed10[i * 5 + 3] = Long.valueOf((long) (Math.random() * 1000000));
    IValueMeta meta3 = new ValueMetaInteger(CONST_STRINGS + (i * 5 + 1), 8, 0);
    metaMixed10.addValueMeta(meta3);

    rowMixed10[i * 5 + 4] = Boolean.valueOf(Math.random() > 0.5);
    IValueMeta meta4 = new ValueMetaBoolean(CONST_STRINGS + (i * 5 + 1));
    metaMixed10.addValueMeta(meta4);
  }

  public long runTestStrings10(int iterations) throws HopValueException {
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      metaString10.cloneRow(rowString10);
    }

    long stopTime = System.currentTimeMillis();

    return stopTime - startTime;
  }

  public long runTestMixed10(int iterations) throws HopValueException {
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      metaMixed10.cloneRow(rowMixed10);
    }

    long stopTime = System.currentTimeMillis();

    return stopTime - startTime;
  }

  public long runTestStrings100(int iterations) throws HopValueException {
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      metaString100.cloneRow(rowString100);
    }

    long stopTime = System.currentTimeMillis();

    return stopTime - startTime;
  }

  public long runTestMixed100(int iterations) throws HopValueException {
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      metaMixed100.cloneRow(rowMixed100);
    }

    long stopTime = System.currentTimeMillis();

    return stopTime - startTime;
  }

  public long runTestStrings1000(int iterations) throws HopValueException {
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      metaString1000.cloneRow(rowString1000);
    }

    long stopTime = System.currentTimeMillis();

    return stopTime - startTime;
  }

  public long runTestMixed1000(int iterations) throws HopValueException {
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      metaMixed1000.cloneRow(rowMixed1000);
    }

    long stopTime = System.currentTimeMillis();

    return stopTime - startTime;
  }

  public static final int ITERATIONS = 1000000;

  @SuppressWarnings("java:S106")
  public static void main(String[] args) throws HopValueException {
    SpeedTest speedTest = new SpeedTest();

    long timeString10 = speedTest.runTestStrings10(ITERATIONS);
    System.out.println(
        "Time to run 'String10' test "
            + ITERATIONS
            + CONST_TIMES
            + timeString10
            + CONST_MS
            + (1000 * ITERATIONS / timeString10)
            + CONST_RS);
    long timeMixed10 = speedTest.runTestMixed10(ITERATIONS);
    System.out.println(
        "Time to run 'Mixed10' test "
            + ITERATIONS
            + CONST_TIMES
            + timeMixed10
            + CONST_MS
            + (1000 * ITERATIONS / timeMixed10)
            + CONST_RS);
    System.out.println();

    long timeString100 = speedTest.runTestStrings100(ITERATIONS);
    System.out.println(
        "Time to run 'String100' test "
            + ITERATIONS
            + CONST_TIMES
            + timeString100
            + CONST_MS
            + (1000 * ITERATIONS / timeString100)
            + CONST_RS);
    long timeMixed100 = speedTest.runTestMixed100(ITERATIONS);
    System.out.println(
        "Time to run 'Mixed100' test "
            + ITERATIONS
            + CONST_TIMES
            + timeMixed100
            + CONST_MS
            + (1000 * ITERATIONS / timeMixed100)
            + CONST_RS);
    System.out.println();

    long timeString1000 = speedTest.runTestStrings1000(ITERATIONS);
    System.out.println(
        "Time to run 'String1000' test "
            + ITERATIONS
            + CONST_TIMES
            + timeString1000
            + CONST_MS
            + (1000 * ITERATIONS / timeString1000)
            + CONST_RS);
    long timeMixed1000 = speedTest.runTestMixed1000(ITERATIONS);
    System.out.println(
        "Time to run 'Mixed1000' test "
            + ITERATIONS
            + CONST_TIMES
            + timeMixed1000
            + CONST_MS
            + (1000 * ITERATIONS / timeMixed1000)
            + CONST_RS);
    System.out.println();
  }
}
