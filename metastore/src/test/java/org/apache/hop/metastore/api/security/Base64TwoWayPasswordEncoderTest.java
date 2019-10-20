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

import junit.framework.TestCase;

/**
 * Created by saslan on 10/23/2015.
 */
public class Base64TwoWayPasswordEncoderTest extends TestCase {
  private Base64TwoWayPasswordEncoder base64TwoWayPasswordEncoder;

  public void setUp() throws Exception {
    base64TwoWayPasswordEncoder = new Base64TwoWayPasswordEncoder();
  }

  public void testEncode() throws Exception {
    CharSequence charSequence = "password";
    String encoded = base64TwoWayPasswordEncoder.encode( charSequence );
    assertEquals( encoded, "cGFzc3dvcmQ=" );
    encoded = base64TwoWayPasswordEncoder.encode( "" );
    assertEquals( encoded, "" );
    encoded = base64TwoWayPasswordEncoder.encode( null );
    assertEquals( encoded, null );
  }

  public void testDecode() throws Exception {
    CharSequence charSequence = "cGFzc3dvcmQ=";
    String decoded = base64TwoWayPasswordEncoder.decode( charSequence );
    assertEquals( decoded, "password" );
    decoded = base64TwoWayPasswordEncoder.decode( "" );
    assertEquals( decoded, "" );
    decoded = base64TwoWayPasswordEncoder.decode( null );
    assertEquals( decoded, null );

  }

  public void testMatches() throws Exception {
    String encodedPassword = "cGFzc3dvcmQ=";
    CharSequence rawPassword = "password";
    boolean matches = base64TwoWayPasswordEncoder.matches( rawPassword, encodedPassword );
    assertEquals( matches, true );
    matches = base64TwoWayPasswordEncoder.matches( null, encodedPassword );
    assertEquals( matches, false );
    matches = base64TwoWayPasswordEncoder.matches( rawPassword, null );
    assertEquals( matches, false );

  }
}
