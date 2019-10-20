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

package org.apache.hop.metastore.util;

import org.junit.Before;
import org.junit.Test;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

public class MetaStoreUtilTest {
  private MetaStoreUtil metaStoreUtil;
  private IMetaStore mockMetaStore;
  private IMetaStoreAttribute mockIMetaStoreAttribute;
  private String namespace = "test_namespace";

  @Before
  public void setUp() throws Exception {
    metaStoreUtil = new MetaStoreUtil();
    mockMetaStore = mock( IMetaStore.class );
    mockIMetaStoreAttribute = mock( IMetaStoreAttribute.class );
  }

  @Test
  public void testVerifyNamespaceCreated() throws Exception {
    when( mockMetaStore.namespaceExists( namespace ) ).thenReturn( false );
    metaStoreUtil.verifyNamespaceCreated( mockMetaStore, namespace );
    verify( mockMetaStore ).createNamespace( namespace );
  }

  @Test
  public void testGetChildString() throws Exception {
    IMetaStoreAttribute mockIMetaStoreAttributeChild = mock( IMetaStoreAttribute.class );
    when( mockIMetaStoreAttribute.getChild( "id" ) ).thenReturn( mockIMetaStoreAttributeChild );
    when( mockIMetaStoreAttributeChild.getValue() ).thenReturn( "attrString" );
    String childString = MetaStoreUtil.getChildString( mockIMetaStoreAttribute, "id" );
    assertEquals( childString, "attrString" );
  }

  @Test
  public void testGetAttributeBoolean() throws Exception {
    boolean attrBool = MetaStoreUtil.getAttributeBoolean( mockIMetaStoreAttribute, "id" );
    assertFalse( attrBool );
  }

  @Test
  public void testGetElementNames() throws Exception {
    IMetaStoreElementType metaStoreElementType = mock( IMetaStoreElementType.class );

    List<IMetaStoreElement> elements = new ArrayList<>();
    IMetaStoreElement elem1 = mock( IMetaStoreElement.class );
    elements.add( elem1 );

    when( mockMetaStore.getElements( namespace, metaStoreElementType ) ).thenReturn( elements );
    when( elem1.getName() ).thenReturn( "test" );

    String[] names = metaStoreUtil.getElementNames( namespace, mockMetaStore, metaStoreElementType );
    assertEquals( names.length, 1 );
  }

  @Test
  public void testCopyElements_empty() throws Exception {
    IMetaStore from = new MemoryMetaStore();
    IMetaStore to = new MemoryMetaStore();

    MetaStoreUtil.copy( from, to );

    assertEquals( from.getNamespaces().size(), to.getNamespaces().size() );
  }

  @Test
  public void testCopy() throws Exception {
    IMetaStore from = mock( IMetaStore.class );
    IMetaStore to = mock( IMetaStore.class );

    String[] namespaces = new String[] { "hop", "apache" };
    List<IMetaStoreElementType> penElementTypes = new ArrayList<>();
    IMetaStoreElementType type1 = mock( IMetaStoreElementType.class );
    IMetaStoreElementType type2 = mock( IMetaStoreElementType.class );
    penElementTypes.add( type1 );
    penElementTypes.add( type2 );

    List<IMetaStoreElement> elements = new ArrayList<>();
    IMetaStoreElement elem1 = mock( IMetaStoreElement.class );
    IMetaStoreElement elem2 = mock( IMetaStoreElement.class );
    IMetaStoreElement elem3 = mock( IMetaStoreElement.class );
    elements.add( elem1 );
    elements.add( elem2 );
    elements.add( elem3 );

    when( from.getNamespaces() ).thenReturn( Arrays.asList( namespaces ) );
    when( from.getElementTypes( "hop" ) ).thenReturn( penElementTypes );
    when( from.getElements( "hop", type1 ) ).thenReturn( elements );
    when( from.getElements( "hop", type2 ) ).thenReturn( elements );

    MetaStoreUtil.copy( from, to );

    verify( to ).createNamespace( "hop" );
    verify( to ).createNamespace( "apache" );
    verify( to ).createElementType( "hop", type1 );
    verify( to ).createElementType( "hop", type2 );
    verify( to ).createElement( "hop", type1, elem1 );
    verify( to ).createElement( "hop", type1, elem2 );
    verify( to ).createElement( "hop", type1, elem3 );
    verify( to ).createElement( "hop", type2, elem1 );
    verify( to ).createElement( "hop", type2, elem2 );
    verify( to ).createElement( "hop", type2, elem3 );

    verify( to, never() ).createElementType( eq( "apache" ), any( IMetaStoreElementType.class ) );
    verify( to, never() )
      .createElement( eq( "apache" ), any( IMetaStoreElementType.class ), any( IMetaStoreElement.class ) );
  }

  @Test
  public void testCopy_existingElementType_overwriteFalse() throws Exception {
    IMetaStore from = mock( IMetaStore.class );
    IMetaStore to = mock( IMetaStore.class );

    String[] namespaces = new String[] { "hop", "apache" };
    List<IMetaStoreElementType> penElementTypes = new ArrayList<>();
    IMetaStoreElementType type1 = mock( IMetaStoreElementType.class );
    IMetaStoreElementType type2 = mock( IMetaStoreElementType.class );
    when( type1.getName() ).thenReturn( "type1" );
    when( type2.getName() ).thenReturn( "type2" );
    penElementTypes.add( type1 );
    penElementTypes.add( type2 );

    List<IMetaStoreElement> elements = new ArrayList<>();
    IMetaStoreElement elem1 = mock( IMetaStoreElement.class );
    IMetaStoreElement elem2 = mock( IMetaStoreElement.class );
    IMetaStoreElement elem3 = mock( IMetaStoreElement.class );
    elements.add( elem1 );
    elements.add( elem2 );
    elements.add( elem3 );

    when( from.getNamespaces() ).thenReturn( Arrays.asList( namespaces ) );
    when( from.getElementTypes( "hop" ) ).thenReturn( penElementTypes );
    when( from.getElements( "hop", type1 ) ).thenReturn( elements );
    when( from.getElements( "hop", type2 ) ).thenReturn( elements );

    // set up an existing element type
    IMetaStoreElementType existingType = mock( IMetaStoreElementType.class );
    when( to.getElementTypeByName( anyString(), anyString() ) ).thenReturn( existingType );
    when( existingType.getId() ).thenReturn( "existingID" );

    MetaStoreUtil.copy( from, to, false );

    verify( to ).createNamespace( "hop" );
    verify( to ).createNamespace( "apache" );
    verify( to, never() ).createElementType( "hop", type1 );
    verify( to, never() ).createElementType( "hop", type2 );

    verify( type1, never() ).setId( "existingID" );
    verify( type2, never() ).setId( "existingID" );
    verify( to, never() ).updateElementType( "hop", type1 );
    verify( to, never() ).updateElementType( "hop", type2 );

    verify( to, never() ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem1 ) );
    verify( to, never() ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem2 ) );
    verify( to, never() ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem3 ) );

    verify( to, never() )
      .updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), anyString(), eq( elem1 ) );
    verify( to, never() )
      .updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), anyString(), eq( elem2 ) );
    verify( to, never() )
      .updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), anyString(), eq( elem3 ) );

    verify( to, never() ).createElementType( eq( "apache" ), any( IMetaStoreElementType.class ) );
    verify( to, never() ).createElement( eq( "apache" ), any( IMetaStoreElementType.class ), any( IMetaStoreElement.class ) );

  }

  @Test
  public void testCopy_existingElementType_overwriteTrue() throws Exception {
    IMetaStore from = mock( IMetaStore.class );
    IMetaStore to = mock( IMetaStore.class );

    String[] namespaces = new String[] { "hop", "apache" };
    List<IMetaStoreElementType> penElementTypes = new ArrayList<>();
    IMetaStoreElementType type1 = mock( IMetaStoreElementType.class );
    IMetaStoreElementType type2 = mock( IMetaStoreElementType.class );
    when( type1.getName() ).thenReturn( "type1" );
    when( type2.getName() ).thenReturn( "type2" );
    penElementTypes.add( type1 );
    penElementTypes.add( type2 );

    List<IMetaStoreElement> elements = new ArrayList<>();
    IMetaStoreElement elem1 = mock( IMetaStoreElement.class );
    IMetaStoreElement elem2 = mock( IMetaStoreElement.class );
    IMetaStoreElement elem3 = mock( IMetaStoreElement.class );
    elements.add( elem1 );
    elements.add( elem2 );
    elements.add( elem3 );

    when( from.getNamespaces() ).thenReturn( Arrays.asList( namespaces ) );
    when( from.getElementTypes( "hop" ) ).thenReturn( penElementTypes );
    when( from.getElements( "hop", type1 ) ).thenReturn( elements );
    when( from.getElements( "hop", type2 ) ).thenReturn( elements );

    // set up an existing element type
    IMetaStoreElementType existingType = mock( IMetaStoreElementType.class );
    when( to.getElementTypeByName( anyString(), anyString() ) ).thenReturn( existingType );
    when( existingType.getId() ).thenReturn( "existingID" );

    MetaStoreUtil.copy( from, to, true );

    verify( to ).createNamespace( "hop" );
    verify( to ).createNamespace( "apache" );
    verify( to, never() ).createElementType( "hop", type1 );
    verify( to, never() ).createElementType( "hop", type2 );

    verify( type1 ).setId( "existingID" );
    verify( type2 ).setId( "existingID" );
    verify( to ).updateElementType( "hop", type1 );
    verify( to ).updateElementType( "hop", type2 );

    verify( to, times( 2 ) ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem1 ) );
    verify( to, times( 2 ) ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem2 ) );
    verify( to, times( 2 ) ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem3 ) );

    verify( to, never() )
      .updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), anyString(), eq( elem1 ) );
    verify( to, never() )
      .updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), anyString(), eq( elem2 ) );
    verify( to, never() )
      .updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), anyString(), eq( elem3 ) );

    verify( to, never() ).createElementType( eq( "apache" ), any( IMetaStoreElementType.class ) );
    verify( to, never() ).createElement( eq( "apache" ), any( IMetaStoreElementType.class ), any( IMetaStoreElement.class ) );

  }

  @Test
  public void testCopy_existingElementTypeAndElement_overwriteTrue() throws Exception {
    IMetaStore from = mock( IMetaStore.class );
    IMetaStore to = mock( IMetaStore.class );

    String[] namespaces = new String[] { "hop", "apache" };
    List<IMetaStoreElementType> penElementTypes = new ArrayList<>();
    IMetaStoreElementType type1 = mock( IMetaStoreElementType.class );
    when( type1.getName() ).thenReturn( "type1" );
    penElementTypes.add( type1 );

    List<IMetaStoreElement> elements = new ArrayList<>();
    IMetaStoreElement elem1 = mock( IMetaStoreElement.class );
    IMetaStoreElement elem2 = mock( IMetaStoreElement.class );
    IMetaStoreElement elem3 = mock( IMetaStoreElement.class );
    when( elem1.getId() ).thenReturn( "elementID" );
    when( elem1.getName() ).thenReturn( "elementName" );
    elements.add( elem1 );
    elements.add( elem2 );
    elements.add( elem3 );

    when( from.getNamespaces() ).thenReturn( Arrays.asList( namespaces ) );
    when( from.getElementTypes( "hop" ) ).thenReturn( penElementTypes );
    when( from.getElements( eq( "hop" ), any( IMetaStoreElementType.class ) ) ).thenReturn( elements );

    // set up an existing element type
    IMetaStoreElementType existingType = mock( IMetaStoreElementType.class );
    when( to.getElementTypeByName( anyString(), anyString() ) ).thenReturn( existingType );
    when( existingType.getId() ).thenReturn( "existingID" );

    when( to.getElementByName( eq( "hop" ), any( IMetaStoreElementType.class ), eq( "elementName" ) ) ).thenReturn( elem1 );

    MetaStoreUtil.copy( from, to, true );

    verify( to ).createNamespace( "hop" );
    verify( to, never() ).createElementType( "hop", type1 );

    verify( type1 ).setId( "existingID" );
    verify( to ).updateElementType( "hop", type1 );

    verify( to, never() ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem1 ) );
    verify( to ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem2 ) );
    verify( to ).createElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( elem3 ) );

    verify( to ).updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), eq( "elementID" ), eq( elem1 ) );
    verify( to, never() )
      .updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), anyString(), eq( elem2 ) );
    verify( to, never() )
      .updateElement( eq( "hop" ), any( IMetaStoreElementType.class ), anyString(), eq( elem3 ) );

    verify( to, never() ).createElementType( eq( "apache" ), any( IMetaStoreElementType.class ) );
    verify( to, never() ).createElement( eq( "apache" ), any( IMetaStoreElementType.class ), any( IMetaStoreElement.class ) );

  }
}
