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

package org.apache.hop.core.encryption;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.eclipse.jetty.util.security.Password;

/**
 * This class handles basic encryption of passwords in Hop. Note that it's not really encryption, it's more
 * obfuscation. Passwords are <b>difficult</b> to read, not impossible.
 *
 * @author Matt
 * @since 17-12-2003
 */
public class Encr {

  private static ITwoWayPasswordEncoder encoder;

  public Encr() {
  }

  @Deprecated
  public boolean init() {
    return true;
  }

  public static void init( String encoderPluginId ) throws HopException {
    if ( Utils.isEmpty( encoderPluginId ) ) {
      throw new HopException( "Unable to initialize the two way password encoder: No encoder plugin type specified." );
    }
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.findPluginWithId( TwoWayPasswordEncoderPluginType.class, encoderPluginId );
    if ( plugin == null ) {
      throw new HopException( "Unable to find plugin with ID '" + encoderPluginId + "'" );
    }
    encoder = (ITwoWayPasswordEncoder) registry.loadClass( plugin );

    // Load encoder specific options...
    //
    encoder.init();
  }

  /**
   * Old signature code, used in license keys, from the Hop 1.x days.
   *
   * @param signature
   * @param verify
   * @return
   * @deprecated
   */
  @Deprecated
  public static final boolean checkSignatureShort( String signature, String verify ) {
    return getSignatureShort( signature ).equalsIgnoreCase( verify );
  }

  /**
   * Old Hop 1.x code used to get a short signature from a long version signature for license checking.
   *
   * @param signature
   * @return
   * @deprecated
   */
  @Deprecated
  public static final String getSignatureShort( String signature ) {
    String retval = "";
    if ( signature == null ) {
      return retval;
    }
    int len = signature.length();
    if ( len < 6 ) {
      return retval;
    }
    retval = signature.substring( len - 5, len );

    return retval;
  }

  public static final String encryptPassword( String password ) {

    return encoder.encode( password, false );

  }

  public static final String decryptPassword( String encrypted ) {

    return encoder.decode( encrypted );

  }

  /**
   * The word that is put before a password to indicate an encrypted form. If this word is not present, the password is
   * considered to be NOT encrypted
   */
  public static final String PASSWORD_ENCRYPTED_PREFIX = "Encrypted ";

  /**
   * Encrypt the password, but only if the password doesn't contain any variables.
   *
   * @param password The password to encrypt
   * @return The encrypted password or the
   */
  public static final String encryptPasswordIfNotUsingVariables( String password ) {

    return encoder.encode( password, true );
  }

  /**
   * Decrypts a password if it contains the prefix "Encrypted "
   *
   * @param password The encrypted password
   * @return The decrypted password or the original value if the password doesn't start with "Encrypted "
   */
  public static final String decryptPasswordOptionallyEncrypted( String password ) {

    return encoder.decode( password, true );
  }

  /**
   * Gets encoder
   *
   * @return value of encoder
   */
  public static ITwoWayPasswordEncoder getEncoder() {
    return encoder;
  }

  /**
   * Create an encrypted password
   *
   * @param args the password to encrypt
   */
  public static void main( String[] args ) throws HopException {
    HopClientEnvironment.init();
    if ( args.length != 2 ) {
      printOptions();
      System.exit( 9 );
    }

    String option = args[ 0 ];
    String password = args[ 1 ];

    if ( Const.trim( option ).substring( 1 ).equalsIgnoreCase( "hop" ) ) {
      // Hop password obfuscation
      //
      try {
        String obfuscated = Encr.encryptPasswordIfNotUsingVariables( password );
        System.out.println(obfuscated);
        System.exit( 0 );
      } catch ( Exception ex ) {
        System.err.println( "Error encrypting password" );
        ex.printStackTrace();
        System.exit( 2 );
      }

    } else if ( Const.trim( option ).substring( 1 ).equalsIgnoreCase( "server" ) ) {
      // Jetty password obfuscation
      //
      String obfuscated = Password.obfuscate( password );
      System.out.println(obfuscated);
      System.exit( 0 );

    } else {
      // Unknown option, print usage
      //
      System.err.println( "Unknown option '" + option + "'\n" );
      printOptions();
      System.exit( 1 );
    }

  }

  private static void printOptions() {
    System.err.println( "encr usage:\n" );
    System.err.println( "  encr <-hop|-server> <password>" );
    System.err.println( "  Options:" );
    System.err.println( "    -hop: generate an obfuscated password to include in Hop XML files" );
    System.err.println( "    -server : generate an obfuscated password to include in the hop-server password file 'pwd/hop.pwd'" );
    System.err.println( "\nThis command line tool obfuscates a plain text password for use in XML and password files." );
    System.err.println( "Make sure to also copy the '" + PASSWORD_ENCRYPTED_PREFIX + "' prefix to indicate the obfuscated nature of the password." );
    System.err.println( "Hop will then be able to make the distinction between regular plain text passwords and obfuscated ones." );
    System.err.println();
  }
}
