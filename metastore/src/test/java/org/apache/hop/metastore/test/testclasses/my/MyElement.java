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

import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@MetaStoreElementType(
  name = "My element type",
  description = "This is my element type" )
public class MyElement {
  public static final String LIST_KEY_MY_NAMES = "MyNames";
  public static final String LIST_KEY_MY_FILENAMES = "MyFilenames";
  public static final String FACTORY_OTHER_ELEMENT = "MyOtherElementFactory";

  private String name;

  @MetaStoreAttribute( key = "my_attribute" )
  private String myAttribute;

  @MetaStoreAttribute
  private String anotherAttribute;

  @MetaStoreAttribute( password = true )
  private String passwordAttribute;

  @MetaStoreAttribute
  private int intAttribute;

  @MetaStoreAttribute
  private long longAttribute;

  @MetaStoreAttribute
  private boolean boolAttribute;

  @MetaStoreAttribute
  private Date dateAttribute;

  @MetaStoreAttribute
  private List<MyElementAttr> subAttributes;

  @MetaStoreAttribute
  private List<String> stringList;

  @MetaStoreAttribute( nameReference = true, nameListKey = LIST_KEY_MY_NAMES )
  private MyNameElement nameElement;

  @MetaStoreAttribute( filenameReference = true, filenameListKey = LIST_KEY_MY_FILENAMES )
  private MyFilenameElement filenameElement;

  @MetaStoreAttribute( factoryNameReference = true, factoryNameKey = FACTORY_OTHER_ELEMENT )
  private MyOtherElement myOtherElement;

  /**
   * We need the empty constructor for our factory.  Having any other constructor is fine too, but this one is mandatory!
   */
  public MyElement() {
    subAttributes = new ArrayList<MyElementAttr>();
    stringList = new ArrayList<String>();
  }

  public MyElement( String name, String myAttribute, String anotherAttribute, String passwordAttribute, int intAttribute, long longAttribute, boolean boolAttribute, Date dateAttribute ) {
    this();
    this.name = name;
    this.myAttribute = myAttribute;
    this.anotherAttribute = anotherAttribute;
    this.passwordAttribute = passwordAttribute;
    this.intAttribute = intAttribute;
    this.longAttribute = longAttribute;
    this.boolAttribute = boolAttribute;
    this.dateAttribute = dateAttribute;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getMyAttribute() {
    return myAttribute;
  }

  public void setMyAttribute( String myAttribute ) {
    this.myAttribute = myAttribute;
  }

  public String getAnotherAttribute() {
    return anotherAttribute;
  }

  public void setAnotherAttribute( String anotherAttribute ) {
    this.anotherAttribute = anotherAttribute;
  }

  public int getIntAttribute() {
    return intAttribute;
  }

  public void setIntAttribute( int intAttribute ) {
    this.intAttribute = intAttribute;
  }

  public boolean isBoolAttribute() {
    return boolAttribute;
  }

  public void setBoolAttribute( boolean boolAttribute ) {
    this.boolAttribute = boolAttribute;
  }

  public long getLongAttribute() {
    return longAttribute;
  }

  public void setLongAttribute( long longAttribute ) {
    this.longAttribute = longAttribute;
  }

  public Date getDateAttribute() {
    return dateAttribute;
  }

  public void setDateAttribute( Date dateAttribute ) {
    this.dateAttribute = dateAttribute;
  }

  public String getPasswordAttribute() {
    return passwordAttribute;
  }

  public void setPasswordAttribute( String passwordAttribute ) {
    this.passwordAttribute = passwordAttribute;
  }

  public List<MyElementAttr> getSubAttributes() {
    return subAttributes;
  }

  public void setSubAttributes( List<MyElementAttr> subAttributes ) {
    this.subAttributes = subAttributes;
  }

  public MyNameElement getNameElement() {
    return nameElement;
  }

  public void setNameElement( MyNameElement nameElement ) {
    this.nameElement = nameElement;
  }

  public MyFilenameElement getFilenameElement() {
    return filenameElement;
  }

  public void setFilenameElement( MyFilenameElement filenameElement ) {
    this.filenameElement = filenameElement;
  }

  /**
   * @return the stringList
   */
  public List<String> getStringList() {
    return stringList;
  }

  /**
   * @param stringList the stringList to set
   */
  public void setStringList( List<String> stringList ) {
    this.stringList = stringList;
  }

  /**
   * @return the myOtherElement
   */
  public MyOtherElement getMyOtherElement() {
    return myOtherElement;
  }

  /**
   * @param myOtherElement the myOtherElement to set
   */
  public void setMyOtherElement( MyOtherElement myOtherElement ) {
    this.myOtherElement = myOtherElement;
  }
}
