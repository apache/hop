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

package org.apache.hop.core.compress.gzip;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * The GZIP compression levels 1 to 9: lower is faster, higher gives smaller files. Used for the
 * temporary files of transforms such as Sort rows and Blocking transform.
 */
public enum GzipCompressionLevel implements IEnumHasCodeAndDescription {
  FASTEST(1),
  VERY_FAST(2),
  FAST(3),
  BALANCED_FASTER(4),
  BALANCED(5),
  STANDARD(6),
  SMALLER(7),
  VERY_SMALL(8),
  SMALLEST(9);

  private static final Class<?> PKG = GzipCompressionLevel.class;

  /**
   * The level of a newly created transform: temporary files are read back only once, so speed
   * matters most.
   */
  public static final GzipCompressionLevel NEW_TRANSFORM_DEFAULT = FASTEST;

  /**
   * The level of a transform saved before the level could be chosen: the level those transforms
   * always used, so they keep working the same way.
   */
  public static final GzipCompressionLevel NOT_STORED_DEFAULT = STANDARD;

  private final int level;

  GzipCompressionLevel(int level) {
    this.level = level;
  }

  /** The level passed to {@link java.util.zip.Deflater}, from 1 to 9. */
  public int getLevel() {
    return level;
  }

  /** The constant name: this is what transforms store in their metadata. */
  @Override
  public String getCode() {
    return name();
  }

  /** The level number and its name, for example "1 - Fastest". */
  @Override
  public String getDescription() {
    return level + " - " + BaseMessages.getString(PKG, "GzipCompressionLevel." + name());
  }

  /**
   * @param level a stored level, or null when none was stored (metadata from before the option)
   * @return the level, or {@link #NOT_STORED_DEFAULT} when it is null
   */
  public static GzipCompressionLevel orNotStoredDefault(GzipCompressionLevel level) {
    return level == null ? NOT_STORED_DEFAULT : level;
  }

  /**
   * Opens a GZIP stream that compresses at this level.
   *
   * @param out the stream to write the compressed data to
   * @return the GZIP stream
   * @throws IOException when the GZIP header cannot be written
   */
  public GZIPOutputStream newOutputStream(OutputStream out) throws IOException {
    return new GZIPOutputStream(out) {
      {
        def.setLevel(level);
      }
    };
  }
}
