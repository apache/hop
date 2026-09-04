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

package org.apache.hop.setup.persist;

import java.io.InputStream;
import java.util.List;

public class SystemProcessRunner implements IProcessRunner {

  @Override
  public int run(List<String> command) throws Exception {
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    // A child reading its standard input only exits once it sees EOF, so release the write end
    // immediately: powershell.exe otherwise keeps waiting for more commands and never terminates.
    process.getOutputStream().close();
    try (InputStream in = process.getInputStream()) {
      in.readAllBytes();
    }
    return process.waitFor();
  }
}
