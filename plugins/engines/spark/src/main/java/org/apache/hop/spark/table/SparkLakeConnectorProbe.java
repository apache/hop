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

package org.apache.hop.spark.table;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.hop.core.exception.HopException;

/**
 * Verifies that optional lakehouse connector classes are loadable from a given classloader (the
 * native Spark engine plugin classloader for hop-run / GUI {@code local[*]}, or the driver
 * classpath under spark-submit).
 *
 * <p>Connectors are <strong>not</strong> shipped in the default Hop assembly. Operators install
 * jars under {@code plugins/engines/spark/lib/} (and restart Hop) or supply them via {@code
 * spark-submit --packages}. See {@code plugins/engines/spark/README.md}.
 */
public final class SparkLakeConnectorProbe {

  private SparkLakeConnectorProbe() {}

  /**
   * Ensures every requested format has its primary extension class on {@code classLoader}.
   *
   * @param formats format ids ({@link SparkLakeFormats#FORMAT_DELTA}, {@link
   *     SparkLakeFormats#FORMAT_ICEBERG}); null/blank entries ignored; unknown formats fail
   * @param classLoader classloader that will load Spark session / DataSource classes (typically the
   *     engine plugin CL)
   * @throws HopException if a required connector class is missing, with install / --packages recipe
   */
  public static void verifyClasspath(Collection<String> formats, ClassLoader classLoader)
      throws HopException {
    Objects.requireNonNull(classLoader, "classLoader");
    for (String format : normalizeFormats(formats)) {
      switch (format) {
        case SparkLakeFormats.FORMAT_DELTA ->
            requireClass(
                classLoader,
                SparkLakeFormats.DELTA_EXTENSION,
                missingDeltaMessage(SparkLakeFormats.DELTA_EXTENSION));
        case SparkLakeFormats.FORMAT_ICEBERG ->
            requireClass(
                classLoader,
                SparkLakeFormats.ICEBERG_EXTENSIONS,
                missingIcebergMessage(SparkLakeFormats.ICEBERG_EXTENSIONS));
        default ->
            throw new HopException(
                "Unsupported lakehouse table format '"
                    + format
                    + "'. Supported: "
                    + SparkLakeFormats.FORMAT_DELTA
                    + ", "
                    + SparkLakeFormats.FORMAT_ICEBERG
                    + ".");
      }
    }
  }

  /**
   * Like {@link #verifyClasspath(Collection, ClassLoader)} using the probe's own classloader
   * (engine plugin CL when this class is loaded from the spark engine jar).
   */
  public static void verifyClasspath(Collection<String> formats) throws HopException {
    verifyClasspath(formats, SparkLakeConnectorProbe.class.getClassLoader());
  }

  /** True if the given class is loadable (no initialization). */
  public static boolean isClassPresent(String className, ClassLoader classLoader) {
    Objects.requireNonNull(className, "className");
    Objects.requireNonNull(classLoader, "classLoader");
    try {
      Class.forName(className, false, classLoader);
      return true;
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return false;
    }
  }

  public static boolean isClassPresent(String className) {
    return isClassPresent(className, SparkLakeConnectorProbe.class.getClassLoader());
  }

  public static boolean isDeltaPresent(ClassLoader classLoader) {
    return isClassPresent(SparkLakeFormats.DELTA_EXTENSION, classLoader);
  }

  public static boolean isIcebergPresent(ClassLoader classLoader) {
    return isClassPresent(SparkLakeFormats.ICEBERG_EXTENSIONS, classLoader);
  }

  /**
   * Actionable message when the Delta connector is missing from the engine classpath.
   *
   * <p>Public for unit tests and for callers that want to surface the recipe without probing.
   */
  public static String missingDeltaMessage(String missingClass) {
    return "Delta Lake connector not on the native Spark engine classpath (missing class: "
        + missingClass
        + "). Place delta-spark_4.1_2.13 (and its Delta-specific dependencies) under "
        + "plugins/engines/spark/lib/delta/ (or plugins/engines/spark/lib/), then restart Hop. "
        + "For spark-submit use: --packages io.delta:delta-spark_4.1_2.13:4.3.1 "
        + "(see docs: plugins/engines/spark/README.md lakehouse section).";
  }

  /** Actionable message when the Iceberg connector is missing from the engine classpath. */
  public static String missingIcebergMessage(String missingClass) {
    return "Apache Iceberg connector not on the native Spark engine classpath (missing class: "
        + missingClass
        + "). Place iceberg-spark-runtime-4.1_2.13 under "
        + "plugins/engines/spark/lib/iceberg/ (or plugins/engines/spark/lib/), then restart Hop. "
        + "For spark-submit use: --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 "
        + "(see docs: plugins/engines/spark/README.md lakehouse section).";
  }

  private static void requireClass(ClassLoader classLoader, String className, String message)
      throws HopException {
    if (!isClassPresent(className, classLoader)) {
      throw new HopException(message);
    }
  }

  private static Set<String> normalizeFormats(Collection<String> formats) {
    Set<String> out = new LinkedHashSet<>();
    if (formats == null) {
      return out;
    }
    for (String raw : formats) {
      if (raw == null) {
        continue;
      }
      String f = raw.trim().toLowerCase(Locale.ROOT);
      if (!f.isEmpty()) {
        out.add(f);
      }
    }
    return out;
  }
}
