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

package org.apache.hop.pipeline.transforms.tokenreplacement;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.Arrays;

/**
 * Reader for in place token replacements. Does not use as much memory as the String.replace() method.
 */
public class TokenReplacingReader extends Reader {

  private String tokenStartMarker;
  private String tokenEndMarker;
  private char[] tokenStartMarkerChars;
  private char[] tokenEndMarkerChars;
  private char[] tmpTokenStartMarkerChars;
  private char[] tmpTokenEndMarkerChars;
  private PushbackReader pushbackReader;
  private TokenResolver tokenResolver;
  private StringBuilder tokenBuffer = new StringBuilder();
  private String resolvedToken = null;
  private int resolvedTokenIndex = 0;

  public TokenReplacingReader(final TokenResolver resolver, final Reader source, final String tokenStartMarker,
                              final String tokenEndMarker) {
    if (resolver == null) {
      throw new IllegalArgumentException("Token resolver may not be null");
    }

    if ((tokenStartMarker == null || tokenStartMarker.length() < 1)
      || (tokenEndMarker == null || tokenEndMarker.length() < 1)) {
      throw new IllegalArgumentException("Token start / end marker may not be null or empty");
    }

    this.tokenStartMarker = tokenStartMarker;
    this.tokenEndMarker = tokenEndMarker;
    this.tokenStartMarkerChars = tokenStartMarker.toCharArray();
    this.tokenEndMarkerChars = tokenEndMarker.toCharArray();
    this.tmpTokenStartMarkerChars = new char[tokenStartMarker.length()];
    this.tmpTokenEndMarkerChars = new char[tokenEndMarker.length()];
    this.pushbackReader = new PushbackReader(source, Math.max(tokenStartMarker.length(), tokenEndMarker.length()));
    this.tokenResolver = resolver;
  }

  @Override
  public int read() throws IOException {
    if (resolvedToken != null) {
      if (resolvedTokenIndex < resolvedToken.length()) {
        return resolvedToken.charAt(resolvedTokenIndex++);
      }

      if (resolvedTokenIndex == resolvedToken.length()) {
        resolvedToken = null;
        resolvedTokenIndex = 0;
      }
    }

    // read proper number of chars into a temp. char array in order to find token start marker
    int countValidChars = readChars(tmpTokenStartMarkerChars);

    if (!Arrays.equals(tmpTokenStartMarkerChars, tokenStartMarkerChars)) {
      if (countValidChars > 0) {
        pushbackReader.unread(tmpTokenStartMarkerChars, 0, countValidChars);
      }

      return pushbackReader.read();
    }

    // found start of token, read proper number of chars into a temp. char array in order to find token end marker
    boolean endOfSource = false;
    boolean invalidToken = false;
    tokenBuffer.delete(0, tokenBuffer.length());
    countValidChars = readChars(tmpTokenEndMarkerChars);

    while (!Arrays.equals(tmpTokenEndMarkerChars, tokenEndMarkerChars)) {
      if (countValidChars == -1) {
        // end of source and no token end marker was found
        endOfSource = true;

        break;
      }

      tokenBuffer.append(tmpTokenEndMarkerChars[0]);

      pushbackReader.unread(tmpTokenEndMarkerChars, 0, countValidChars);
      if (pushbackReader.read() == -1) {
        // end of source and no token end marker was found
        endOfSource = true;

        break;
      }

      countValidChars = readChars(tmpTokenEndMarkerChars);
    }

    if (endOfSource) {
      resolvedToken = tokenStartMarker + tokenBuffer.toString();
    } else {
      // try to resolve token
      resolvedToken = tokenResolver.resolveToken(tokenBuffer.toString());
      if (resolvedToken == null) {
        // token was not resolved
        resolvedToken = tokenStartMarker + tokenBuffer.toString() + tokenEndMarker;
      }
    }

    if( resolvedToken.length() > resolvedTokenIndex ) {
      return resolvedToken.charAt( resolvedTokenIndex++ );
    } else {
      return -1;
    }

  }

  private int readChars(char[] tmpChars) throws IOException {
    int countValidChars = -1;
    int length = tmpChars.length;
    int data = pushbackReader.read();

    for (int i = 0; i < length; i++) {
      if (data != -1) {
        tmpChars[i] = (char) data;
        countValidChars = i + 1;
        if (i + 1 < length) {
          data = pushbackReader.read();
        }
      } else {
        // reset to java default value for char
        tmpChars[i] = '\u0000';
      }
    }

    return countValidChars;
  }

  @Override
  public int read(char[] cbuf) throws IOException {
    return read(cbuf, 0, cbuf.length);
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    int charsRead = 0;
    for (int i = 0; i < len; i++) {
      int nextChar = read();
      if (nextChar == -1) {
        charsRead = i;
        if (charsRead == 0) {
          charsRead = -1;
        }

        break;
      } else {
        charsRead = i + 1;
      }

      cbuf[off + i] = (char) nextChar;
    }

    return charsRead;
  }

  @Override
  public void close() throws IOException {
    this.pushbackReader.close();
  }

  @Override
  public boolean ready() throws IOException {
    return this.pushbackReader.ready();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public int read(CharBuffer target) throws IOException {
    throw new UnsupportedOperationException("Method int read(CharBuffer target) is not supported");
  }

  @Override
  public long skip(long n) throws IOException {
    throw new UnsupportedOperationException("Method long skip(long n) is not supported");
  }

  @Override
  public void mark(int readAheadLimit) throws IOException {
    throw new UnsupportedOperationException("Method void mark(int readAheadLimit) is not supported");
  }

  @Override
  public void reset() throws IOException {
    throw new UnsupportedOperationException("Method void reset() is not supported");
  }
}