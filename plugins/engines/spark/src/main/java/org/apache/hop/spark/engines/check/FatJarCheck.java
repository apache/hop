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

package org.apache.hop.spark.engines.check;

import java.io.File;
import java.util.zip.ZipFile;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.vfs.HopVfs;

/**
 * The fat jar the executors load: Hop builds one when none is set. A jar that is set must exist,
 * and must not hold Spark itself, which would clash with the Spark of the cluster.
 */
public class FatJarCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "FatJar";

  /** A class every Spark runtime has: a fat jar with it holds Spark itself. */
  static final String SPARK_CLASS = "org/apache/spark/SparkContext.class";

  public FatJarCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.isOnACluster();
  }

  @Override
  public ICheckResult run(SparkCheckContext context) throws Exception {
    String location = context.resolve(context.getConfiguration().getFatJar());
    if (location.isEmpty()) {
      return ok(
          "Not set: Hop builds a fat jar of this installation without Spark inside when the"
              + " pipeline starts, and keeps it in the fat jar cache for the next runs.");
    }
    FileObject jar = HopVfs.getFileObject(location, context.getVariables());
    if (!jar.exists()) {
      return error(
          "The fat jar '" + location + "' does not exist.",
          "Correct the location, or leave it empty so Hop builds the fat jar.");
    }
    long size = jar.getContent().getSize();
    String found = "'" + location + "' exists (" + (size / (1024 * 1024)) + " MB).";
    File local = localFile(jar);
    if (local == null) {
      return ok(found);
    }
    try (ZipFile zip = new ZipFile(local)) {
      if (zip.getEntry(SPARK_CLASS) != null) {
        return warning(
            found + " It holds Spark itself, which clashes with the Spark of the cluster.",
            "Use a fat jar without Spark inside (native-provided), or leave the location empty so"
                + " Hop builds one.");
      }
    }
    return ok(found + " It holds no Spark of its own.");
  }

  private static File localFile(FileObject file) {
    try {
      return "file".equals(file.getName().getScheme()) ? new File(file.getName().getPath()) : null;
    } catch (Exception e) {
      return null;
    }
  }
}
