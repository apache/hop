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

package org.apache.hop.pipeline.transforms.mqtt.key;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.mqtt.publisher.MQTTPublisherMeta;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateCrtKeySpec;

/**
 * Class for reading RSA private key from PEM file.
 * <p>
 * <p/>It can read PEM files with PKCS#8 or PKCS#1 encodings.
 * It doesn't support encrypted PEM files.
 */
public class PrivateKeyReader {

  // Private key file using PKCS #1 encoding
  public static final String P1_BEGIN_MARKER = "-----BEGIN RSA PRIVATE KEY";
  public static final String P1_END_MARKER = "-----END RSA PRIVATE KEY";

  // Private key file using PKCS #8 encoding
  public static final String P8_BEGIN_MARKER = "-----BEGIN PRIVATE KEY";
  public static final String P8_END_MARKER = "-----END PRIVATE KEY";

  /**
   * Read the private key from a PEM file
   *
   * @param pFile the PEM file
   * @return the {@code PrivateKey} instance
   * @throws IOException              if a problem occurs
   * @throws NoSuchAlgorithmException if a problem occurs
   */
  protected static PrivateKey readKey( File pFile ) throws IOException, NoSuchAlgorithmException {

    KeyFactory factory = KeyFactory.getInstance( "RSA" );
    BufferedReader br = null;
    try {
      br = new BufferedReader( new FileReader( pFile ) );
      String line = null;
      boolean reading = false;
      while ( ( line = br.readLine() ) != null ) {
        if ( line.contains( P1_BEGIN_MARKER ) ) {
          byte[] keyBytes = readKeyMaterial( P1_END_MARKER, br );
          RSAPrivateCrtKeySpec keySpec = getRSAKeySpec( keyBytes );

          try {
            return factory.generatePrivate( keySpec );
          } catch ( InvalidKeySpecException e ) {
            throw new IOException(
                BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidPKCS1PEMFile" ) + e
                    .getMessage() );
          }
        }

        if ( line.contains( P8_BEGIN_MARKER ) ) {
          byte[] keyBytes = readKeyMaterial( P8_END_MARKER, br );
          EncodedKeySpec keySpec = new PKCS8EncodedKeySpec( keyBytes );

          try {
            return factory.generatePrivate( keySpec );
          } catch ( InvalidKeySpecException e ) {
            throw new IOException(
                BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidPKCS8PEMFile" ) + e
                    .getMessage() );
          }
        }
      }

      throw new IOException(
          BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidPEMFileNoBeginMarker" ) );
    } finally {
      if ( br != null ) {
        br.close();
      }
    }
  }

  /**
   * Read the stuff between the start and end markers of a key file
   *
   * @param endMarker the end marker to watch for
   * @param reader    the reader to read from
   * @return decoded base64 bytes
   * @throws IOException if a problem occurs
   */
  private static byte[] readKeyMaterial( String endMarker, BufferedReader reader ) throws IOException {
    String line = null;
    StringBuffer buf = new StringBuffer();

    while ( ( line = reader.readLine() ) != null ) {
      if ( line.contains( endMarker ) ) {

        return DatatypeConverter.parseBase64Binary( buf.toString() );
      }

      buf.append( line.trim() );
    }

    throw new IOException(
        BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidPEMFileNoEndMarker" ) );
  }

  /**
   * Convert PKCS#1 encoded private key into RSAPrivateCrtKeySpec.
   * <p>
   * <p/>The ASN.1 syntax for the private key with CRT is
   * <p>
   * <pre>
   * --
   * -- Representation of RSA private key with information for the CRT algorithm.
   * --
   * RSAPrivateKey ::= SEQUENCE {
   *   version           Version,
   *   modulus           INTEGER,  -- n
   *   publicExponent    INTEGER,  -- e
   *   privateExponent   INTEGER,  -- d
   *   prime1            INTEGER,  -- p
   *   prime2            INTEGER,  -- q
   *   exponent1         INTEGER,  -- d mod (p-1)
   *   exponent2         INTEGER,  -- d mod (q-1)
   *   coefficient       INTEGER,  -- (inverse of q) mod p
   *   otherPrimeInfos   OtherPrimeInfos OPTIONAL
   * }
   * </pre>
   *
   * @param keyBytes PKCS#1 encoded key
   * @return KeySpec
   * @throws IOException
   */

  private static RSAPrivateCrtKeySpec getRSAKeySpec( byte[] keyBytes ) throws IOException {

    DerParser parser = new DerParser( keyBytes );

    Asn1Object sequence = parser.read();
    if ( sequence.getType() != DerParser.SEQUENCE ) {
      throw new IOException(
          BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERNotASequence" ) );
    }

    // Parse inside the sequence
    parser = sequence.getParser();

    parser.read(); // Skip version
    BigInteger modulus = parser.read().getInteger();
    BigInteger publicExp = parser.read().getInteger();
    BigInteger privateExp = parser.read().getInteger();
    BigInteger prime1 = parser.read().getInteger();
    BigInteger prime2 = parser.read().getInteger();
    BigInteger exp1 = parser.read().getInteger();
    BigInteger exp2 = parser.read().getInteger();
    BigInteger crtCoef = parser.read().getInteger();

    RSAPrivateCrtKeySpec
        keySpec =
        new RSAPrivateCrtKeySpec( modulus, publicExp, privateExp, prime1, prime2, exp1, exp2, crtCoef );

    return keySpec;
  }

  /**
   * A bare-minimum ASN.1 DER decoder, just having enough functions to
   * decode PKCS#1 private keys. Especially, it doesn't handle explicitly
   * tagged types with an outer tag.
   * <p>
   * <p/>This parser can only handle one layer. To parse nested constructs,
   * get a new parser for each layer using <code>Asn1Object.getParser()</code>.
   * <p>
   * <p/>There are many DER decoders in JRE but using them will tie this
   * program to a specific JCE/JVM.
   *
   * @author zhang
   */
  private static class DerParser {

    // Classes
    public static final int UNIVERSAL = 0x00;
    public static final int APPLICATION = 0x40;
    public static final int CONTEXT = 0x80;
    public static final int PRIVATE = 0xC0;

    // Constructed Flag
    public static final int CONSTRUCTED = 0x20;

    // Tag and data types
    public static final int ANY = 0x00;
    public static final int BOOLEAN = 0x01;
    public static final int INTEGER = 0x02;
    public static final int BIT_STRING = 0x03;
    public static final int OCTET_STRING = 0x04;
    public static final int NULL = 0x05;
    public static final int OBJECT_IDENTIFIER = 0x06;
    public static final int REAL = 0x09;
    public static final int ENUMERATED = 0x0a;
    public static final int RELATIVE_OID = 0x0d;

    public static final int SEQUENCE = 0x10;
    public static final int SET = 0x11;

    public static final int NUMERIC_STRING = 0x12;
    public static final int PRINTABLE_STRING = 0x13;
    public static final int T61_STRING = 0x14;
    public static final int VIDEOTEX_STRING = 0x15;
    public static final int IA5_STRING = 0x16;
    public static final int GRAPHIC_STRING = 0x19;
    public static final int ISO646_STRING = 0x1A;
    public static final int GENERAL_STRING = 0x1B;

    public static final int UTF8_STRING = 0x0C;
    public static final int UNIVERSAL_STRING = 0x1C;
    public static final int BMP_STRING = 0x1E;

    public static final int UTC_TIME = 0x17;
    public static final int GENERALIZED_TIME = 0x18;

    protected InputStream in;

    /**
     * Create a new DER decoder from an input stream.
     *
     * @param in The DER encoded stream
     */
    public DerParser( InputStream in ) throws IOException {
      this.in = in;
    }

    /**
     * Create a new DER decoder from a byte array.
     *
     * @param bytes encoded bytes
     * @throws IOException
     */
    public DerParser( byte[] bytes ) throws IOException {
      this( new ByteArrayInputStream( bytes ) );
    }

    /**
     * Read next object. If it's constructed, the value holds
     * encoded content and it should be parsed by a new
     * parser from <code>Asn1Object.getParser</code>.
     *
     * @return A object
     * @throws IOException
     */
    public Asn1Object read() throws IOException {
      int tag = in.read();

      if ( tag == -1 ) {
        throw new IOException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERStreamToShortMissingTag" ) );
      }

      int length = getLength();

      byte[] value = new byte[length];
      int n = in.read( value );
      if ( n < length ) {
        throw new IOException( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERStreamToShortMissingValue" ) );
      }

      Asn1Object o = new Asn1Object( tag, length, value );

      return o;
    }

    /**
     * Decode the length of the field. Can only support length
     * encoding up to 4 octets.
     * <p>
     * <p/>In BER/DER encoding, length can be encoded in 2 forms,
     * <ul>
     * <li>Short form. One octet. Bit 8 has value "0" and bits 7-1
     * give the length.
     * <li>Long form. Two to 127 octets (only 4 is supported here).
     * Bit 8 of first octet has value "1" and bits 7-1 give the
     * number of additional length octets. Second and following
     * octets give the length, base 256, most significant digit first.
     * </ul>
     *
     * @return The length as integer
     * @throws IOException
     */
    private int getLength() throws IOException {

      int i = in.read();
      if ( i == -1 ) {
        throw new IOException( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERLengthMissing" ) );
      }

      // A single byte short length
      if ( ( i & ~0x7F ) == 0 ) {
        return i;
      }

      int num = i & 0x7F;

      // We can't handle length longer than 4 bytes
      if ( i >= 0xFF || num > 4 ) {
        throw new IOException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERFieldTooBig", i ) );
      }

      byte[] bytes = new byte[num];
      int n = in.read( bytes );
      if ( n < num ) {
        throw new IOException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERLengthTooShort" ) );
      }

      return new BigInteger( 1, bytes ).intValue();
    }
  }

  /**
   * An ASN.1 TLV. The object is not parsed. It can
   * only handle integers and strings.
   *
   * @author zhang
   */
  private static class Asn1Object {

    protected final int type;
    protected final int length;
    protected final byte[] value;
    protected final int tag;

    /**
     * Construct a ASN.1 TLV. The TLV could be either a
     * constructed or primitive entity.
     * <p>
     * <p/>The first byte in DER encoding is made of following fields,
     * <pre>
     * -------------------------------------------------
     * |Bit 8|Bit 7|Bit 6|Bit 5|Bit 4|Bit 3|Bit 2|Bit 1|
     * -------------------------------------------------
     * |  Class    | CF  |     +      Type             |
     * -------------------------------------------------
     * </pre>
     * <ul>
     * <li>Class: Universal, Application, Context or Private
     * <li>CF: Constructed flag. If 1, the field is constructed.
     * <li>Type: This is actually called tag in ASN.1. It
     * indicates data type (Integer, String) or a construct
     * (sequence, choice, set).
     * </ul>
     *
     * @param tag    Tag or Identifier
     * @param length Length of the field
     * @param value  Encoded octet string for the field.
     */
    public Asn1Object( int tag, int length, byte[] value ) {
      this.tag = tag;
      this.type = tag & 0x1F;
      this.length = length;
      this.value = value;
    }

    public int getType() {
      return type;
    }

    public int getLength() {
      return length;
    }

    public byte[] getValue() {
      return value;
    }

    public boolean isConstructed() {
      return ( tag & DerParser.CONSTRUCTED ) == DerParser.CONSTRUCTED;
    }

    /**
     * For constructed field, return a parser for its content.
     *
     * @return A parser for the construct.
     * @throws IOException
     */
    public DerParser getParser() throws IOException {
      if ( !isConstructed() ) {
        throw new IOException( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERCantParsePrimitive" ) );
      }

      return new DerParser( value );
    }

    /**
     * Get the value as integer
     *
     * @return BigInteger
     * @throws IOException
     */
    public BigInteger getInteger() throws IOException {
      if ( type != DerParser.INTEGER ) {
        throw new IOException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERObjectIsNotAnInteger" ) );
      }

      return new BigInteger( value );
    }

    /**
     * Get value as string. Most strings are treated
     * as Latin-1.
     *
     * @return Java string
     * @throws IOException
     */
    public String getString() throws IOException {

      String encoding;

      switch ( type ) {

        // Not all are Latin-1 but it's the closest thing
        case DerParser.NUMERIC_STRING:
        case DerParser.PRINTABLE_STRING:
        case DerParser.VIDEOTEX_STRING:
        case DerParser.IA5_STRING:
        case DerParser.GRAPHIC_STRING:
        case DerParser.ISO646_STRING:
        case DerParser.GENERAL_STRING:
          encoding = "ISO-8859-1"; //$NON-NLS-1$
          break;

        case DerParser.BMP_STRING:
          encoding = "UTF-16BE"; //$NON-NLS-1$
          break;

        case DerParser.UTF8_STRING:
          encoding = "UTF-8"; //$NON-NLS-1$
          break;

        case DerParser.UNIVERSAL_STRING:
          throw new IOException( BaseMessages
              .getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERCantHandleUCS4String" ) );

        default:
          throw new IOException( BaseMessages
              .getString( MQTTPublisherMeta.PKG, "MQTTClientSSL.Error.InvalidDERObjectIsNotAString" ) );
      }

      return new String( value, encoding );
    }
  }
}
