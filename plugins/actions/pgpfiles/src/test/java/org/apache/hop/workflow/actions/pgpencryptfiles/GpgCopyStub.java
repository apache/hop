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

package org.apache.hop.workflow.actions.pgpencryptfiles;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Assumptions;

/**
 * Stands in for {@code gpg}. Copies the file operand that follows {@code --} onto the {@code
 * --output} path, and records the argument vector and standard input next to itself.
 */
final class GpgCopyStub {

  private GpgCopyStub() {}

  static Path install(Path directory) throws Exception {
    Files.createDirectories(directory);
    if (System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win")) {
      return compileWindows(directory);
    }
    return writeShell(directory);
  }

  static List<String> arguments(Path directory) throws Exception {
    return Files.readAllLines(directory.resolve("args.txt"), StandardCharsets.UTF_8);
  }

  static String standardInput(Path directory) throws Exception {
    return Files.readString(directory.resolve("stdin.txt"), StandardCharsets.UTF_8);
  }

  private static Path compileWindows(Path directory) throws Exception {
    Path csc = Path.of("C:/Windows/Microsoft.NET/Framework64/v4.0.30319/csc.exe");
    if (!Files.isRegularFile(csc)) {
      csc = Path.of("C:/Windows/Microsoft.NET/Framework/v4.0.30319/csc.exe");
    }
    Assumptions.assumeTrue(Files.isRegularFile(csc), "csc.exe is required to build the GnuPG stub");
    Path source = directory.resolve("GpgCopyStub.cs");
    Path exe = directory.resolve("gpg-copy-stub.exe");
    Files.writeString(source, windowsSource(), StandardCharsets.UTF_8);
    Process process =
        new ProcessBuilder(
                csc.toString(), "/nologo", "/out:" + exe.toAbsolutePath(), source.toString())
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    int code = process.waitFor();
    if (code != 0) {
      throw new IllegalStateException("csc failed (" + code + "): " + output);
    }
    return exe;
  }

  private static Path writeShell(Path directory) throws Exception {
    Path script = directory.resolve("gpg-copy-stub.sh");
    Files.writeString(script, shellSource(), StandardCharsets.UTF_8);
    script.toFile().setExecutable(true);
    return script;
  }

  private static String shellSource() {
    return """
        #!/bin/sh
        dir=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
        { for a in "$@"; do printf '%s\\n' "$a"; done; } > "$dir/args.txt"
        out=
        src=
        while [ $# -gt 0 ]; do
          case "$1" in
            --output) out="$2"; shift 2 ;;
            --) shift; src="$1"; break ;;
            *) shift ;;
          esac
        done
        if [ -n "$out" ] && [ -n "$src" ]; then
          cp "$src" "$out"
        fi
        cat > "$dir/stdin.txt"
        exit 0
        """;
  }

  private static String windowsSource() {
    return """
        using System;
        using System.IO;
        using System.Text;
        class GpgCopyStub {
          static int Main(string[] args) {
            string dir = Path.GetDirectoryName(
                System.Reflection.Assembly.GetExecutingAssembly().Location);
            File.WriteAllLines(Path.Combine(dir, "args.txt"), args);
            string output = null;
            string input = null;
            for (int i = 0; i < args.Length; i++) {
              if (args[i] == "--output" && i + 1 < args.Length) output = args[++i];
              else if (args[i] == "--" && i + 1 < args.Length) {
                input = args[i + 1];
                break;
              }
            }
            if (output != null && input != null) File.Copy(input, output, true);
            using (var reader = new StreamReader(Console.OpenStandardInput(), Encoding.UTF8)) {
              File.WriteAllText(Path.Combine(dir, "stdin.txt"), reader.ReadToEnd());
            }
            return 0;
          }
        }
        """;
  }
}
