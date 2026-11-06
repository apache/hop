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

package org.apache.hop.pipeline.transforms.cube;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The data streams on top of a cube file's GZIP stream. Rows are written and read a few bytes at a
 * time; without a buffer in between, each of those is its own deflate or inflate call.
 */
public final class CubeStreams {

  /** The size of the buffer between the data stream and the GZIP stream. */
  public static final int BUFFER_SIZE = 64 * 1024;

  private CubeStreams() {}

  /** Closing the returned stream flushes the buffer and finishes the GZIP stream. */
  public static DataOutputStream dataOutput(GZIPOutputStream gzip) {
    return new DataOutputStream(new BufferedOutputStream(gzip, BUFFER_SIZE));
  }

  public static DataInputStream dataInput(GZIPInputStream gzip) {
    return new DataInputStream(new BufferedInputStream(gzip, BUFFER_SIZE));
  }
}
