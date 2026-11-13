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

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

/** A check with a translated name, and the results it gives. */
public abstract class BaseSparkConfigurationCheck implements ISparkConfigurationCheck {

  private static final Class<?> PKG = BaseSparkConfigurationCheck.class;

  private final String id;

  protected BaseSparkConfigurationCheck(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  /** The name, from SparkCheck.&lt;id&gt;.Name in the messages. */
  @Override
  public String getName() {
    return BaseMessages.getString(PKG, "SparkCheck." + id + ".Name");
  }

  @Override
  public String getDescription() {
    return getName();
  }

  protected ICheckResult ok(String found) {
    return new CheckResult(ICheckResult.TYPE_RESULT_OK, found, this);
  }

  /** Something to know that is neither good nor bad. */
  protected ICheckResult info(String found) {
    return new CheckResult(ICheckResult.TYPE_RESULT_COMMENT, found, this);
  }

  protected ICheckResult warning(String found, String fix) {
    return new CheckResult(ICheckResult.TYPE_RESULT_WARNING, withFix(found, fix), this);
  }

  protected ICheckResult error(String found, String fix) {
    return new CheckResult(ICheckResult.TYPE_RESULT_ERROR, withFix(found, fix), this);
  }

  static String withFix(String found, String fix) {
    return fix == null || fix.isEmpty()
        ? found
        : found + " " + BaseMessages.getString(PKG, "SparkCheck.Fix") + " " + fix;
  }

  /**
   * Writes a small file in {@code folder} and deletes it again, through Hop's file systems (named
   * connections included).
   *
   * @throws Exception when the folder can't be written, with the reason
   */
  protected static void probeWrite(String folder, IVariables variables) throws Exception {
    FileObject dir = HopVfs.getFileObject(folder, variables);
    if (!dir.exists()) {
      throw new IllegalStateException("the folder does not exist");
    }
    FileObject probe = dir.resolveFile("hop-configuration-test-" + UUID.randomUUID() + ".tmp");
    try (OutputStream out = HopVfs.getOutputStream(probe, false)) {
      out.write("Hop configuration test".getBytes(StandardCharsets.UTF_8));
    } finally {
      probe.delete();
    }
  }
}
