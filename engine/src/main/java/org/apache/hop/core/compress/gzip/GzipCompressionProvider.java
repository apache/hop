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

import org.apache.hop.core.compress.CompressionPlugin;
import org.apache.hop.core.compress.ICompressionProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@CompressionPlugin(
  id = "GZIP",
  name = "GZip",
  description = "GZIP compression"
)
public class GzipCompressionProvider implements ICompressionProvider {

  @Override
  public GzipCompressionInputStream createInputStream( InputStream in ) throws IOException {
    return new GzipCompressionInputStream( in, this );
  }

  @Override
  public boolean supportsInput() {
    return true;
  }

  @Override
  public GzipCompressionOutputStream createOutputStream( OutputStream out ) throws IOException {
    return new GzipCompressionOutputStream( out, this );
  }

  @Override
  public boolean supportsOutput() {
    return true;
  }

  @Override
  public String getDescription() {
    return "GZIP compression";
  }

  @Override
  public String getName() {
    return "GZip";
  }

  @Override
  public String getDefaultExtension() {
    return "gz";
  }
}
