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
 *
 */

package org.apache.hop.encryption;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.plugin.HopSubCommand;
import org.apache.hop.hop.plugin.IHopSubCommand;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.eclipse.jetty.util.security.Password;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Getter
@Setter
@HopSubCommand(id = "encrypt", description = "Encrypt or obfuscate a secret")
@Command(mixinStandardHelpOptions = true, description = "Encrypt secrets")
public class HopSubCommandEncrypt implements Runnable, IHopSubCommand {
  @CommandLine.Option(
      names = {"-hop", "--hop"},
      description =
          "Generate an obfuscated or encrypted password (depending on the configuration), "
              + "for use in XML, password or metadata files")
  protected boolean usingHop;

  @CommandLine.Option(
      names = {"-server", "--server"},
      description =
          "generate an obfuscated password to include in the hop-server password file 'pwd/hop.pwd'")
  protected boolean usingServer;

  @CommandLine.Unmatched protected String[] passwords;

  protected CommandLine cmd;

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
  }

  @Override
  public void run() {
    try {
      if (passwords == null || passwords.length == 0) {
        System.exit(1);
      }

      if (isUsingHop()) {
        for (String password : passwords) {
          String obfuscated = Encr.encryptPasswordIfNotUsingVariables(password);
          System.out.println(password + " --> [" + obfuscated + "]");
        }
        System.exit(0);
      } else if (isUsingServer()) {
        // Jetty password obfuscation
        //
        for (String password : passwords) {
          String obfuscated = Password.obfuscate(password);
          System.out.println(password + " --> [" + obfuscated + "]");
        }
        System.exit(0);
      } else {
        cmd.printVersionHelp(System.out);
        System.exit(1);
      }
    } catch (Exception ex) {
      System.err.println("Error encrypting password");
      ex.printStackTrace();
      System.exit(2);
    }
  }
}
