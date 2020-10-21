/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.core.encryption;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * This class handles basic encryption of passwords in Hop. Note that it's not really encryption, it's more
 * obfuscation. Passwords are <b>difficult</b> to read, not impossible.
 *
 * @author Matt
 * @since 17-12-2003
 */
public class HopTwoWayPasswordEncoder implements ITwoWayPasswordEncoder {
  private static final HopTwoWayPasswordEncoder instance = new HopTwoWayPasswordEncoder();
  private static final int RADIX = 16;
  private String Seed;
  /**
   * The word that is put before a password to indicate an encrypted form. If this word is not present, the password is
   * considered to be NOT encrypted
   */
  public static final String PASSWORD_ENCRYPTED_PREFIX = "Encrypted ";

  public HopTwoWayPasswordEncoder() {
    String envSeed = Const.NVL( EnvUtil.getSystemProperty( Const.HOP_TWO_WAY_PASSWORD_ENCODER_SEED ), "0933910847463829827159347601486730416058" ); // Solve for PDI-16512
    Seed = envSeed;
  }

  private static HopTwoWayPasswordEncoder getInstance() {
    return instance;
  }

  @Override
  public void init() throws HopException {
    // Nothing to do here.
  }

  @Override
  public String encode( String rawPassword ) {
    return encode( rawPassword, true );
  }

  @Override
  public String encode( String rawPassword, boolean includePrefix ) {
    if ( includePrefix ) {
      return encryptPasswordIfNotUsingVariablesInternal( rawPassword );
    } else {
      return encryptPasswordInternal( rawPassword );
    }
  }

  @Override
  public String decode( String encodedPassword ) {

    if ( encodedPassword != null && encodedPassword.startsWith( PASSWORD_ENCRYPTED_PREFIX ) ) {
      encodedPassword = encodedPassword.substring( PASSWORD_ENCRYPTED_PREFIX.length() );
    }

    return decryptPasswordInternal( encodedPassword );
  }

  @Override
  public String decode( String encodedPassword, boolean optionallyEncrypted ) {

    if ( encodedPassword == null ) {
      return null;
    }

    if ( optionallyEncrypted ) {

      if ( encodedPassword.startsWith( PASSWORD_ENCRYPTED_PREFIX ) ) {
        encodedPassword = encodedPassword.substring( PASSWORD_ENCRYPTED_PREFIX.length() );
        return decryptPasswordInternal( encodedPassword );
      } else {
        return encodedPassword;
      }
    } else {
      return decryptPasswordInternal( encodedPassword );
    }
  }

  @VisibleForTesting
  protected String encryptPasswordInternal( String password ) {
    if ( password == null ) {
      return "";
    }
    if ( password.length() == 0 ) {
      return "";
    }

    BigInteger biPasswd = new BigInteger( password.getBytes() );

    BigInteger biR0 = new BigInteger( getSeed() );
    BigInteger biR1 = biR0.xor( biPasswd );

    return biR1.toString( RADIX );
  }

  @VisibleForTesting
  protected String decryptPasswordInternal( String encrypted ) {
    if ( encrypted == null ) {
      return "";
    }
    if ( encrypted.length() == 0 ) {
      return "";
    }

    BigInteger biConfuse = new BigInteger( getSeed() );

    try {
      BigInteger biR1 = new BigInteger( encrypted, RADIX );
      BigInteger biR0 = biR1.xor( biConfuse );

      return new String( biR0.toByteArray() );
    } catch ( Exception e ) {
      return "";
    }
  }


  @VisibleForTesting
  protected String getSeed() {
    return this.Seed;
  }

  @Override
  public String[] getPrefixes() {
    return new String[] { PASSWORD_ENCRYPTED_PREFIX };
  }


  /**
   * Encrypt the password, but only if the password doesn't contain any variables.
   *
   * @param password The password to encrypt
   * @return The encrypted password or the
   */
  protected final String encryptPasswordIfNotUsingVariablesInternal( String password ) {
    String encrPassword = "";
    List<String> varList = new ArrayList<>();
    StringUtil.getUsedVariables( password, varList, true );
    if ( varList.isEmpty() ) {
      encrPassword = PASSWORD_ENCRYPTED_PREFIX + HopTwoWayPasswordEncoder.encryptPassword( password );
    } else {
      encrPassword = password;
    }

    return encrPassword;
  }


  /**
   * Decrypts a password if it contains the prefix "Encrypted "
   *
   * @param password The encrypted password
   * @return The decrypted password or the original value if the password doesn't start with "Encrypted "
   */
  protected final String decryptPasswordOptionallyEncryptedInternal( String password ) {
    if ( !Utils.isEmpty( password ) && password.startsWith( PASSWORD_ENCRYPTED_PREFIX ) ) {
      return HopTwoWayPasswordEncoder.decryptPassword( password.substring( PASSWORD_ENCRYPTED_PREFIX.length() ) );
    }
    return password;
  }

  // Old Static Methods - should be deprecated


  /**
   * Encrypt the password, but only if the password doesn't contain any variables.
   *
   * @param password The password to encrypt
   * @return The encrypted password or the
   * @deprecated - Use the instance method through Encr instead of this directly
   */
  public static final String encryptPasswordIfNotUsingVariables( String password ) {
    return getInstance().encryptPasswordIfNotUsingVariablesInternal( password );
  }

  /**
   * Decrypts a password if it contains the prefix "Encrypted "
   *
   * @param password The encrypted password
   * @return The decrypted password or the original value if the password doesn't start with "Encrypted "
   * @deprecated - Use the instance method through Encr instead of this directly
   */
  public static final String decryptPasswordOptionallyEncrypted( String password ) {
    return getInstance().decryptPasswordOptionallyEncryptedInternal( password );
  }

  /**
   * Deprecared - use the instance method instead
   *
   * @param password
   * @return encrypted password
   * @deprecated - use the instance method through Encr instead of this directly
   */
  public static final String encryptPassword( String password ) {
    return getInstance().encryptPasswordInternal( password );
  }

  /**
   * Deprecated - use Encr instead of this directly
   *
   * @param encrypted
   * @return decrypted password
   * @deprecated
   */
  public static final String decryptPassword( String encrypted ) {
    return getInstance().decryptPasswordInternal( encrypted );
  }


}
