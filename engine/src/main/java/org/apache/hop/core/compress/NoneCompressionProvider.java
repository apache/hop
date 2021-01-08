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

package org.apache.hop.core.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@CompressionPlugin(
  id = "None",
  name = "None",
  description = "None"
)
public class NoneCompressionProvider implements ICompressionProvider {

  @Override
  public CompressionInputStream createInputStream( InputStream in ) throws IOException {
    return new NoneCompressionInputStream( in, this );
  }

  @Override
  public boolean supportsInput() {
    return true;
  }

  @Override
  public CompressionOutputStream createOutputStream( OutputStream out ) throws IOException {
    return new NoneCompressionOutputStream( out, this );
  }

  @Override
  public boolean supportsOutput() {
    return true;
  }

  /**
   * Gets the description for the compression provider.
   *
   * @return a description of the provider. For core plugins this needs to match what's in the XML file
   */
  @Override
  public String getDescription() {
    return "No compression";
  }

  /**
   * Gets the name for the compression provider.
   *
   * @return the name of the provider. For core plugins this needs to match what's in the XML file
   */
  @Override
  public String getName() {
    return "None";
  }

  @Override
  public String getDefaultExtension() {
    return null;
  }

  public static class NoneCompressionInputStream extends CompressionInputStream {

    public NoneCompressionInputStream( InputStream in, ICompressionProvider provider ) {
      super( in, provider );
    }

  }

  public static class NoneCompressionOutputStream extends CompressionOutputStream {

    public NoneCompressionOutputStream( OutputStream out, ICompressionProvider provider ) {
      super( out, provider );
    }
  }
}
