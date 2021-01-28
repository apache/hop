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

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.eclipse.jetty.util.security.Password;

public class HopEncrypt {
  /**
   * Create an encrypted password
   *
   * @param args the password to encrypt
   */
  public static void main(String[] args) throws HopException {
    HopEnvironment.init();
    if (args.length != 2) {
      printOptions();
      System.exit(9);
    }

    String option = args[0];
    String password = args[1];

    if (Const.trim(option).substring(1).equalsIgnoreCase("hop")) {
      // Hop password obfuscation
      //
      try {
        String obfuscated = Encr.encryptPasswordIfNotUsingVariables( password );
        System.out.println(obfuscated);
        System.exit(0);
      } catch (Exception ex) {
        System.err.println("Error encrypting password");
        ex.printStackTrace();
        System.exit(2);
      }

    } else if (Const.trim(option).substring(1).equalsIgnoreCase("server")) {
      // Jetty password obfuscation
      //
      String obfuscated = Password.obfuscate(password);
      System.out.println(obfuscated);
      System.exit(0);

    } else {
      // Unknown option, print usage
      //
      System.err.println("Unknown option '" + option + "'\n");
      printOptions();
      System.exit(1);
    }
  }

  private static void printOptions() {
    System.err.println("hop-encrypt usage:\n");
    System.err.println("  encr <-hop|-server> <password>");
    System.err.println("  Options:");
    System.err.println("    -hop: generate an obfuscated or encrypted password");
    System.err.println(
        "    -server : generate an obfuscated password to include in the hop-server password file 'pwd/hop.pwd'");
    System.err.println(
        "\nThis command line tool obfuscates or encrypts a plain text password for use in XML, password or metadata files.");
    System.err.println(
        "Make sure to also copy the password encryption prefix to indicate the obfuscated nature of the password.");
    System.err.println(
        "Hop will then be able to make the distinction between regular plain text passwords and obfuscated ones.");
    System.err.println();
  }
}
