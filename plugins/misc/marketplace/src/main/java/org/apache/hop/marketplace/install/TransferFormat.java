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

package org.apache.hop.marketplace.install;

import java.util.Locale;

/**
 * Shared number formatting for download progress, so the GUI dialog and the CLI report the same
 * figures the same way.
 *
 * <p>Byte counts come out as symbols ({@code 42.0MB}), which need no translation. Time remaining is
 * returned as an {@link Eta} value rather than a string, because the GUI words it through its
 * message bundle while the CLI prints plain English — only the rounding rules are shared.
 */
public final class TransferFormat {

  private static final String[] BYTE_UNITS = {"KB", "MB", "GB", "TB"};

  private TransferFormat() {}

  /** Unit an {@link Eta} is expressed in. */
  public enum EtaUnit {
    SECONDS,
    MINUTES,
    HOURS
  }

  /**
   * Time remaining, already rounded to the coarsest useful unit.
   *
   * @param amount always at least 1
   * @param unit which unit {@code amount} counts
   */
  public record Eta(long amount, EtaUnit unit) {
    /** True when the amount needs a singular noun ("1 min" rather than "1 mins"). */
    public boolean isSingular() {
      return amount == 1;
    }
  }

  /**
   * A byte count the way a browser shows one: one decimal below 100, none above, so the width stays
   * roughly stable as the number climbs — "42.0MB", then "353MB".
   *
   * <p>Hand-rolled rather than {@code StorageUnitConverter}, which rounds to whole units ("42MB")
   * and so makes a slow download look frozen for seconds at a time.
   */
  public static String bytes(long bytes) {
    if (bytes < 1024L) {
      return bytes + "B";
    }
    double value = bytes;
    int unit = -1;
    do {
      value /= 1024.0;
      unit++;
    } while (value >= 1024.0 && unit < BYTE_UNITS.length - 1);
    return decimals(value) + BYTE_UNITS[unit];
  }

  public static String speed(long bytesPerSecond) {
    return bytes(bytesPerSecond) + "/s";
  }

  /**
   * Locale.ROOT deliberately: a technical size read-out uses a dot, as every download tool does.
   * The default locale would render "17,6MB" on a comma-decimal machine, and worse, group thousands
   * with a dot — making "1.750MB" ambiguous between 1750 and 1.75.
   */
  private static String decimals(double value) {
    return value < 100.0
        ? String.format(Locale.ROOT, "%.1f", value)
        : String.format(Locale.ROOT, "%.0f", Math.floor(value));
  }

  /**
   * Round a number of seconds to the coarsest unit that still says something useful. A precise
   * mm:ss countdown reads as a promise the network cannot keep.
   */
  public static Eta eta(long seconds) {
    if (seconds < 60) {
      return new Eta(Math.max(1, seconds), EtaUnit.SECONDS);
    }
    long minutes = Math.round(seconds / 60.0);
    if (minutes < 60) {
      return new Eta(minutes, EtaUnit.MINUTES);
    }
    return new Eta(Math.round(minutes / 60.0), EtaUnit.HOURS);
  }
}
