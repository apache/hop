/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 */

package org.apache.hop.metastore.api.security;

import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.binary.Base64;

public class Base64TwoWayPasswordEncoder implements ITwoWayPasswordEncoder {

  private String ENCODING = "UTF-8";

  @Override
  public String encode( CharSequence rawPassword ) {
    try {
      if ( rawPassword == null ) {
        return null;
      }
      if ( rawPassword.length() == 0 ) {
        return "";
      }
      return new String( Base64.encodeBase64( rawPassword.toString().getBytes( ENCODING ) ), ENCODING );
    } catch ( UnsupportedEncodingException e ) {
      throw new RuntimeException( ENCODING + " is not a supported encoding: fatal error", e );
    }
  }

  @Override
  public String decode( CharSequence encodedPassword ) {
    try {
      if ( encodedPassword == null ) {
        return null;
      }
      if ( encodedPassword.length() == 0 ) {
        return "";
      }
      return new String( Base64.decodeBase64( encodedPassword.toString() ), ENCODING );
    } catch ( UnsupportedEncodingException e ) {
      throw new RuntimeException( ENCODING + " is not a supported encoding: fatal error", e );
    }
  }

  @Override
  public boolean matches( CharSequence rawPassword, String encodedPassword ) {
    if ( rawPassword == null || rawPassword.length() == 0 ) {
      return ( encodedPassword == null || encodedPassword.length() == 0 );
    } else {
      if ( encodedPassword == null ) {
        return false;
      } else {
        return encode( rawPassword ).equals( encodedPassword );
      }
    }
  }
}
