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
package org.apache.hop.metastore.stores.xml;

import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Ignore
public abstract class XmlMetaStoreCacheTestBase {

  private XmlMetaStoreCache simpleXmlMetaStoreCache;

  @Before
  public void before() {
    simpleXmlMetaStoreCache = createMetaStoreCache();
  }

  protected abstract XmlMetaStoreCache createMetaStoreCache();

  @Test
  public void registerElementTypeIdForName() {
    simpleXmlMetaStoreCache.registerElementTypeIdForName( "testNamespace", "testElementTypeName", "testElementTypeId" );
    String actualElementId = simpleXmlMetaStoreCache.getElementTypeIdByName( "testNamespace", "testElementTypeName" );
    assertEquals( "testElementTypeId", actualElementId );
  }

  @Test
  public void unregisterElementTypeIdForName() {
    simpleXmlMetaStoreCache.registerElementTypeIdForName( "testNamespace", "testElementTypeName", "testElementTypeId" );
    simpleXmlMetaStoreCache.unregisterElementTypeId( "testNamespace", "testElementTypeId" );
    String actualElementId = simpleXmlMetaStoreCache.getElementTypeIdByName( "testNamespace", "testElementTypeName" );
    assertNull( actualElementId );
  }

  @Test
  public void registerElementIdForName() {
    IMetaStoreElementType testElementType = createTestElementType( "testElementTypeName", "testElementTypeId" );
    IMetaStoreElementType secondElementType = createTestElementType( "secondElementTypeName", "secondElementTypeId" );
    simpleXmlMetaStoreCache.registerElementTypeIdForName( "testNamespace", testElementType.getName(), testElementType.getId() );
    simpleXmlMetaStoreCache.registerElementIdForName( "testNamespace", testElementType, "testElementName", "testElementId" );
    simpleXmlMetaStoreCache.registerElementIdForName( "testNamespace", secondElementType, "secondElementName", "secondElementId" );
    String actualElementId = simpleXmlMetaStoreCache.getElementIdByName( "testNamespace", testElementType, "testElementName" );
    assertEquals( "testElementId", actualElementId );
    String actualSecondElementId = simpleXmlMetaStoreCache.getElementIdByName( "testNamespace", secondElementType, "secondElementName" );
    assertEquals( "secondElementId", actualSecondElementId );
  }

  @Test
  public void registerElementIdForName_for_null_id() {
    IMetaStoreElementType testElementType = createTestElementType( "testElementTypeName", "testElementTypeId" );
    simpleXmlMetaStoreCache.registerElementTypeIdForName( "testNamespace", testElementType.getName(), testElementType.getId() );
    simpleXmlMetaStoreCache.registerElementIdForName( "testNamespace", testElementType, "testElementName", null );
    String actualElementId = simpleXmlMetaStoreCache.getElementIdByName( "testNamespace", testElementType, "testElementName" );
    assertNull( actualElementId );
  }

  @Test
  public void registerElementIdForName_for_non_registered_type() {
    IMetaStoreElementType testElementType = createTestElementType( "testElementTypeName", "testElementTypeId" );
    simpleXmlMetaStoreCache.registerElementIdForName( "testNamespace", testElementType, "testElementName", "testElementId" );
    String actualElementId = simpleXmlMetaStoreCache.getElementIdByName( "testNamespace", testElementType, "testElementName" );
    assertEquals( "testElementId", actualElementId );
  }

  @Test
  public void unregisterElementIdForName() {
    IMetaStoreElementType testElementType = createTestElementType( "testElementTypeName", "testElementTypeId" );
    simpleXmlMetaStoreCache.registerElementTypeIdForName( "testNamespace", testElementType.getName(), testElementType.getId() );
    simpleXmlMetaStoreCache.registerElementIdForName( "testNamespace", testElementType, "testElementName", "testElementId" );
    simpleXmlMetaStoreCache.unregisterElementId( "testNamespace", testElementType, "testElementId" );
    String actualElementId = simpleXmlMetaStoreCache.getElementIdByName( "testNamespace", testElementType, "testElementName" );
    assertNull( "testElementId", actualElementId );
  }

  @Test
  public void registerProcessedFile() {
    simpleXmlMetaStoreCache.registerProcessedFile( "/test/full/Path", 1L );
    Map<String, Long> actualProcessedFiles = simpleXmlMetaStoreCache.getProcessedFiles();
    assertThat( actualProcessedFiles.size(), equalTo( 1 ) );
    assertThat( actualProcessedFiles.containsKey( "/test/full/Path" ), is( true ) );
  }

  @Test
  public void unregisterProcessedFile() {
    simpleXmlMetaStoreCache.registerProcessedFile( "/test/full/Path", 1L );
    simpleXmlMetaStoreCache.unregisterProcessedFile( "/test/full/Path" );
    Map<String, Long> actualProcessedFiles = simpleXmlMetaStoreCache.getProcessedFiles();
    assertThat( actualProcessedFiles.size(), equalTo( 0 ) );
  }

  @Test
  public void clear() {
    simpleXmlMetaStoreCache.registerProcessedFile( "/test/full/Path", 1L );
    IMetaStoreElementType testElementType = createTestElementType( "testElementTypeName", "testElementTypeId" );
    simpleXmlMetaStoreCache.registerElementTypeIdForName( "testNamespace", testElementType.getName(), testElementType.getId() );
    simpleXmlMetaStoreCache.registerElementIdForName( "testNamespace", testElementType, "testElementName", "testElementId" );
    simpleXmlMetaStoreCache.clear();
    Map<String, Long> actualProcessedFiles = simpleXmlMetaStoreCache.getProcessedFiles();
    assertThat( actualProcessedFiles.size(), equalTo( 0 ) );
    String actualElementId = simpleXmlMetaStoreCache.getElementIdByName( "testNamespace", testElementType, "testElementName" );
    assertThat( actualElementId, is( nullValue() ) );
  }

  @Test
  public void clear_empty_cache() {
    simpleXmlMetaStoreCache.clear();
    Map<String, Long> actualProcessedFiles = simpleXmlMetaStoreCache.getProcessedFiles();
    assertThat( actualProcessedFiles.size(), equalTo( 0 ) );
  }

  private static IMetaStoreElementType createTestElementType( String typeName, String typeId ) {
    IMetaStoreElementType testElementType = mock( IMetaStoreElementType.class );
    when( testElementType.getName() ).thenReturn( typeName );
    when( testElementType.getId() ).thenReturn( typeId );
    return testElementType;
  }
}
