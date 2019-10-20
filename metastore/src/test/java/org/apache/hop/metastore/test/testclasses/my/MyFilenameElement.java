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
package org.apache.hop.metastore.test.testclasses.my;

public class MyFilenameElement {
  private String filename;
  private String size;
  private String gender;

  public MyFilenameElement( String filename, String size, String gender ) {
    super();
    this.filename = filename;
    this.size = size;
    this.gender = gender;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof MyFilenameElement ) ) {
      return false;
    }
    MyFilenameElement my = (MyFilenameElement) obj;
    return filename.equals( my.filename ) && size.equals( my.size ) && gender.equals( my.gender );
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getSize() {
    return size;
  }

  public void setSize( String size ) {
    this.size = size;
  }

  public String getGender() {
    return gender;
  }

  public void setGender( String gender ) {
    this.gender = gender;
  }

}
