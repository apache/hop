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

package org.apache.hop.metastore.test;

import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.api.security.MetaStoreElementOwnerType;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DelegatingMetastoreTest {

  @Test
  public void testDelegatingMetaStoreHasNullActiveAfterNoArgConstruction() throws MetaStoreException {
    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore();
    assertNull( delegatingMetaStore.getActiveMetaStore() );
  }

  @Test
  public void testDelegatingMetaStoreHasNullActiveAfterMultiArgConstruction() throws MetaStoreException {
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( mock( IMetaStore.class ), mock( IMetaStore.class ) );
    assertNull( delegatingMetaStore.getActiveMetaStore() );
  }

  @Test
  public void testDelegatingMetaStoreSetActive() throws MetaStoreException {
    final String activeName = "ACTIVE";
    IMetaStore newActiveMetaStore = getMockMetaStoreWithName( activeName );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), newActiveMetaStore,
        getMockMetaStoreWithName( "inactive_2" ) );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    assertEquals( newActiveMetaStore, delegatingMetaStore.getActiveMetaStore() );
  }

  @Test
  public void testAddMetaStoreDoesntChangeActive() throws MetaStoreException {
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), getMockMetaStoreWithName( "inactive_2" ) );
    delegatingMetaStore.addMetaStore( getMockMetaStoreWithName( "new" ) );
    assertNull( delegatingMetaStore.getActiveMetaStore() );
  }

  @Test
  public void testAddMetaStoreAddsMetaStore() throws MetaStoreException {
    final String newName = "new";
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), getMockMetaStoreWithName( "inactive_2" ) );
    IMetaStore newMetaStore = getMockMetaStoreWithName( newName );
    delegatingMetaStore.addMetaStore( newMetaStore );
    assertEquals( 3, delegatingMetaStore.getMetaStoreList().size() );
    assertTrue( delegatingMetaStore.getMetaStoreList().contains( newMetaStore ) );
  }

  @Test
  public void testAddMetaStoreWithIndexDoesntChangeActive() throws MetaStoreException {
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), getMockMetaStoreWithName( "inactive_2" ) );
    delegatingMetaStore.addMetaStore( 1, getMockMetaStoreWithName( "new" ) );
    assertNull( delegatingMetaStore.getActiveMetaStore() );
  }

  @Test
  public void testAddMetaStoreWithIndexAddsMetaStore() throws MetaStoreException {
    final String newName = "new";
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), getMockMetaStoreWithName( "inactive_2" ) );
    IMetaStore newMetaStore = getMockMetaStoreWithName( newName );
    delegatingMetaStore.addMetaStore( 1, newMetaStore );
    assertEquals( 3, delegatingMetaStore.getMetaStoreList().size() );
    assertEquals( 1, delegatingMetaStore.getMetaStoreList().indexOf( newMetaStore ) );
  }

  @Test
  public void testRemoveMetaStoreObjectRemovesMetaStore() throws MetaStoreException {
    final String newName = "new";
    IMetaStore newMetaStore = getMockMetaStoreWithName( newName );
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), newMetaStore,
        getMockMetaStoreWithName( "inactive_2" ) );
    assertEquals( 3, delegatingMetaStore.getMetaStoreList().size() );
    assertTrue( delegatingMetaStore.removeMetaStore( newMetaStore ) );
    assertEquals( 2, delegatingMetaStore.getMetaStoreList().size() );
    assertFalse( delegatingMetaStore.getMetaStoreList().contains( newMetaStore ) );
  }

  @Test
  public void testRemoveMetaStoreNameRemovesMetaStore() throws MetaStoreException {
    final String newName = "new";
    IMetaStore newMetaStore = getMockMetaStoreWithName( newName );
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), newMetaStore,
        getMockMetaStoreWithName( "inactive_2" ) );
    assertEquals( 3, delegatingMetaStore.getMetaStoreList().size() );
    assertTrue( delegatingMetaStore.removeMetaStore( newName ) );
    assertEquals( 2, delegatingMetaStore.getMetaStoreList().size() );
    assertFalse( delegatingMetaStore.getMetaStoreList().contains( newMetaStore ) );
  }

  @Test
  public void testRemoveMetaStoreObjectReturnsFalseIfNotFound() throws MetaStoreException {
    final String newName = "new";
    IMetaStore newMetaStore = getMockMetaStoreWithName( newName );
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), getMockMetaStoreWithName( "inactive_2" ) );
    assertEquals( 2, delegatingMetaStore.getMetaStoreList().size() );
    assertFalse( delegatingMetaStore.removeMetaStore( newMetaStore ) );
  }

  @Test
  public void testRemoveMetaStoreNameReturnsFalseIfNotFound() throws MetaStoreException {
    final String newName = "new";
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), getMockMetaStoreWithName( "inactive_2" ) );
    assertEquals( 2, delegatingMetaStore.getMetaStoreList().size() );
    assertFalse( delegatingMetaStore.removeMetaStore( newName ) );
  }

  @Test
  public void testGetMetaStoreReturnsMetaStore() throws MetaStoreException {
    final String newName = "new";
    IMetaStore newMetaStore = getMockMetaStoreWithName( newName );
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), newMetaStore,
        getMockMetaStoreWithName( "inactive_2" ) );
    assertEquals( newMetaStore, delegatingMetaStore.getMetaStore( newName ) );
  }

  @Test
  public void testGetMetaStoreReturnsNullIfNotThere() throws MetaStoreException {
    final String newName = "new";
    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( "inactive_1" ), getMockMetaStoreWithName( "inactive_2" ) );
    assertNull( delegatingMetaStore.getMetaStore( newName ) );
  }

  @Test
  public void testGetElementTypesReturnsAllIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveType3 = "inactiveType3";
    final String inactiveType4 = "inactiveType4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );
    IMetaStoreElementType inactiveElementType3 = getMockElementTypeWithName( inactiveType3 );
    IMetaStoreElementType inactiveElementType4 = getMockElementTypeWithName( inactiveType4 );

    final List<IMetaStoreElementType> inactive1ElementTypes =
      Arrays.asList( inactiveElementType1, inactiveElementType2 );
    final List<IMetaStoreElementType> inactive2ElementTypes =
      Arrays.asList( inactiveElementType3, inactiveElementType4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    when( inactiveMetaStore1.getElementTypes() ).thenReturn( inactive1ElementTypes );

    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypes() ).thenReturn( inactive2ElementTypes );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, inactiveMetaStore2 );
    List<IMetaStoreElementType> elementTypes = delegatingMetaStore.getElementTypes();
    assertTrue( containsAll( inactive1ElementTypes, elementTypes ) );
    assertTrue( containsAll( inactive2ElementTypes, elementTypes ) );
  }

  @Test
  public void testGetElementTypesReturnsOnlyActiveIfMetaStoreIsActive() throws MetaStoreException {
    final String activeName = "active_1";
    final String inactiveName = "inactive_2";
    final String activeType1 = "activeType1";
    final String activeType2 = "activeType2";
    final String inactiveType3 = "inactiveType3";
    final String inactiveType4 = "inactiveType4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( activeType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( activeType2 );
    IMetaStoreElementType inactiveElementType3 = getMockElementTypeWithName( inactiveType3 );
    IMetaStoreElementType inactiveElementType4 = getMockElementTypeWithName( inactiveType4 );

    final List<IMetaStoreElementType> activeElementTypes = Arrays.asList( inactiveElementType1, inactiveElementType2 );
    final List<IMetaStoreElementType> inactiveElementTypes = Arrays.asList( inactiveElementType3, inactiveElementType4 );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    when( activeMetaStore.getElementTypes() ).thenReturn( activeElementTypes );

    IMetaStore inactiveMetaStore = getMockMetaStoreWithName( inactiveName );
    when( inactiveMetaStore.getElementTypes() ).thenReturn( inactiveElementTypes );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( activeMetaStore, inactiveMetaStore );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    List<IMetaStoreElementType> elementTypes = delegatingMetaStore.getElementTypes();
    assertTrue( containsAll( activeElementTypes, elementTypes ) );
    assertFalse( containsAny( inactiveElementTypes, elementTypes ) );
  }

  @Test
  public void testGetElementTypeIdsReturnsAllIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveType3 = "inactiveType3";
    final String inactiveType4 = "inactiveType4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithIdAndName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithIdAndName( inactiveType2 );
    IMetaStoreElementType inactiveElementType3 = getMockElementTypeWithIdAndName( inactiveType3 );
    IMetaStoreElementType inactiveElementType4 = getMockElementTypeWithIdAndName( inactiveType4 );

    final List<IMetaStoreElementType> inactive1ElementTypes =
      Arrays.asList( inactiveElementType1, inactiveElementType2 );
    final List<IMetaStoreElementType> inactive2ElementTypes =
      Arrays.asList( inactiveElementType3, inactiveElementType4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    when( inactiveMetaStore1.getElementTypes() ).thenReturn( inactive1ElementTypes );

    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypes() ).thenReturn( inactive2ElementTypes );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, inactiveMetaStore2 );
    List<String> elementTypeIds = delegatingMetaStore.getElementTypeIds();
    assertTrue( containsAll( Arrays.asList( inactiveType1, inactiveType2 ), elementTypeIds ) );
    assertTrue( containsAll( Arrays.asList( inactiveType3, inactiveType4 ), elementTypeIds ) );
  }

  @Test
  public void testGetElementTypeIdsReturnsOnlyActiveIfMetaStoreIsActive() throws MetaStoreException {
    final String activeName = "active_1";
    final String inactiveName = "inactive_2";
    final String activeType1 = "activeType1";
    final String activeType2 = "activeType2";
    final String inactiveType3 = "inactiveType3";
    final String inactiveType4 = "inactiveType4";

    IMetaStoreElementType activeElementType1 = getMockElementTypeWithIdAndName( activeType1 );
    IMetaStoreElementType activeElementType2 = getMockElementTypeWithIdAndName( activeType2 );
    IMetaStoreElementType inactiveElementType3 = getMockElementTypeWithIdAndName( inactiveType3 );
    IMetaStoreElementType inactiveElementType4 = getMockElementTypeWithIdAndName( inactiveType4 );

    final List<IMetaStoreElementType> activeElementTypes = Arrays.asList( activeElementType1, activeElementType2 );
    final List<IMetaStoreElementType> inactiveElementTypes = Arrays.asList( inactiveElementType3, inactiveElementType4 );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    when( activeMetaStore.getElementTypes() ).thenReturn( activeElementTypes );

    IMetaStore inactiveMetaStore = getMockMetaStoreWithName( inactiveName );
    when( inactiveMetaStore.getElementTypes() ).thenReturn( inactiveElementTypes );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( activeMetaStore, inactiveMetaStore );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    List<String> elementTypeIds = delegatingMetaStore.getElementTypeIds();
    assertTrue( containsAll( Arrays.asList( activeType1, activeType2 ), elementTypeIds ) );
    assertFalse( containsAny( Arrays.asList( inactiveType3, inactiveType4 ), elementTypeIds ) );
  }

  @Test
  public void testGetElementTypeReturnsTypeIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveType3 = "inactiveType3";
    final String inactiveType4 = "inactiveType4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithIdAndName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithIdAndName( inactiveType2 );
    IMetaStoreElementType inactiveElementType3 = getMockElementTypeWithIdAndName( inactiveType3 );
    IMetaStoreElementType inactiveElementType4 = getMockElementTypeWithIdAndName( inactiveType4 );

    final List<IMetaStoreElementType> inactive1ElementTypes =
      Arrays.asList( inactiveElementType1, inactiveElementType2 );
    final List<IMetaStoreElementType> inactive2ElementTypes =
      Arrays.asList( inactiveElementType3, inactiveElementType4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    when( inactiveMetaStore1.getElementTypes() ).thenReturn( inactive1ElementTypes );

    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypes() ).thenReturn( inactive2ElementTypes );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, inactiveMetaStore2 );
    assertEquals( inactiveElementType1, delegatingMetaStore.getElementType( inactiveType1 ) );
    assertEquals( inactiveElementType2, delegatingMetaStore.getElementType( inactiveType2 ) );
    assertEquals( inactiveElementType3, delegatingMetaStore.getElementType( inactiveType3 ) );
    assertEquals( inactiveElementType4, delegatingMetaStore.getElementType( inactiveType4 ) );
  }

  @Test
  public void testGetElementTypeReturnsActiveTypesIfMetaStoreIsActive() throws MetaStoreException {
    final String activeName = "active_1";
    final String inactiveName = "inactive_2";
    final String activeType1 = "activeType1";
    final String activeType2 = "activeType2";
    final String inactiveType3 = "inactiveType3";
    final String inactiveType4 = "inactiveType4";

    IMetaStoreElementType activeElementType1 = getMockElementTypeWithIdAndName( activeType1 );
    IMetaStoreElementType activeElementType2 = getMockElementTypeWithIdAndName( activeType2 );
    IMetaStoreElementType inactiveElementType3 = getMockElementTypeWithIdAndName( inactiveType3 );
    IMetaStoreElementType inactiveElementType4 = getMockElementTypeWithIdAndName( inactiveType4 );

    final List<IMetaStoreElementType> activeElementTypes = Arrays.asList( activeElementType1, activeElementType2 );
    final List<IMetaStoreElementType> inactiveElementTypes = Arrays.asList( inactiveElementType3, inactiveElementType4 );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    when( activeMetaStore.getElementTypes() ).thenReturn( activeElementTypes );

    IMetaStore inactiveMetaStore = getMockMetaStoreWithName( inactiveName );
    when( inactiveMetaStore.getElementTypes() ).thenReturn( inactiveElementTypes );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( activeMetaStore, inactiveMetaStore );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    assertEquals( activeElementType1, delegatingMetaStore.getElementType( activeType1 ) );
    assertEquals( activeElementType2, delegatingMetaStore.getElementType( activeType2 ) );
    assertNull( delegatingMetaStore.getElementType( inactiveType3 ) );
    assertNull( delegatingMetaStore.getElementType( inactiveType4 ) );
  }

  @Test
  public void testGetElementTypeByNameReturnsTypeIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveType3 = "inactiveType3";
    final String inactiveType4 = "inactiveType4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithIdAndName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithIdAndName( inactiveType2 );
    IMetaStoreElementType inactiveElementType3 = getMockElementTypeWithIdAndName( inactiveType3 );
    IMetaStoreElementType inactiveElementType4 = getMockElementTypeWithIdAndName( inactiveType4 );

    final List<IMetaStoreElementType> inactive1ElementTypes =
      Arrays.asList( inactiveElementType1, inactiveElementType2 );
    final List<IMetaStoreElementType> inactive2ElementTypes =
      Arrays.asList( inactiveElementType3, inactiveElementType4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    when( inactiveMetaStore1.getElementTypes() ).thenReturn( inactive1ElementTypes );

    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypes() ).thenReturn( inactive2ElementTypes );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, inactiveMetaStore2 );
    assertEquals( inactiveElementType1, delegatingMetaStore.getElementTypeByName( inactiveElementType1.getName() ) );
    assertEquals( inactiveElementType2, delegatingMetaStore.getElementTypeByName( inactiveElementType2.getName() ) );
    assertEquals( inactiveElementType3, delegatingMetaStore.getElementTypeByName( inactiveElementType3.getName() ) );
    assertEquals( inactiveElementType4, delegatingMetaStore.getElementTypeByName( inactiveElementType4.getName() ) );
  }

  @Test
  public void testGetElementTypeByNameReturnsActiveTypesIfMetaStoreIsActive() throws MetaStoreException {
    final String activeName = "active_1";
    final String inactiveName = "inactive_2";
    final String activeType1 = "activeType1";
    final String activeType2 = "activeType2";
    final String inactiveType3 = "inactiveType3";
    final String inactiveType4 = "inactiveType4";

    IMetaStoreElementType activeElementType1 = getMockElementTypeWithIdAndName( activeType1 );
    IMetaStoreElementType activeElementType2 = getMockElementTypeWithIdAndName( activeType2 );
    IMetaStoreElementType inactiveElementType3 = getMockElementTypeWithIdAndName( inactiveType3 );
    IMetaStoreElementType inactiveElementType4 = getMockElementTypeWithIdAndName( inactiveType4 );

    final List<IMetaStoreElementType> activeElementTypes = Arrays.asList( activeElementType1, activeElementType2 );
    final List<IMetaStoreElementType> inactiveElementTypes = Arrays.asList( inactiveElementType3, inactiveElementType4 );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    when( activeMetaStore.getElementTypes() ).thenReturn( activeElementTypes );

    IMetaStore inactiveMetaStore = getMockMetaStoreWithName( inactiveName );
    when( inactiveMetaStore.getElementTypes() ).thenReturn( inactiveElementTypes );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( activeMetaStore, inactiveMetaStore );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    assertEquals( activeElementType1, delegatingMetaStore.getElementTypeByName( activeElementType1.getName() ) );
    assertEquals( activeElementType2, delegatingMetaStore.getElementTypeByName( activeElementType2.getName() ) );
    assertNull( delegatingMetaStore.getElementTypeByName( inactiveElementType3.getName() ) );
    assertNull( delegatingMetaStore.getElementTypeByName( inactiveElementType4.getName() ) );
  }

  @Test( expected = MetaStoreException.class )
  public void testCreateElementTypeThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.createElementType( mockElementType );
  }

  @Test
  public void testCreateElementTypeOnlyCreatesElementTypeInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.createElementType( mockElementType );
    verify( activeMetaStore ).createElementType( mockElementType );
    verify( inactiveMetaStore1, never() ).createElementType( any( IMetaStoreElementType.class ) );
    verify( inactiveMetaStore2, never() ).createElementType( any( IMetaStoreElementType.class ) );
  }

  @Test( expected = MetaStoreException.class )
  public void testUpdateElementTypeThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.updateElementType( mockElementType );
  }

  @Test
  public void testUpdateElementTypeOnlyUpdatesElementTypeInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.updateElementType( mockElementType );
    verify( activeMetaStore ).updateElementType( mockElementType );
    verify( inactiveMetaStore1, never() ).updateElementType( any( IMetaStoreElementType.class ) );
    verify( inactiveMetaStore2, never() ).updateElementType( any( IMetaStoreElementType.class ) );
  }

  @Test( expected = MetaStoreException.class )
  public void testDeleteElementTypeThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    
    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.deleteElementType( mockElementType );
  }

  @Test
  public void testDeleteElementTypeOnlyDeletesElementTypeInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";
    

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.deleteElementType( mockElementType );
    verify( activeMetaStore ).deleteElementType( mockElementType );
    verify( inactiveMetaStore1, never() ).deleteElementType( any( IMetaStoreElementType.class ) );
    verify( inactiveMetaStore2, never() ).deleteElementType( any( IMetaStoreElementType.class ) );
  }

  @Test
  public void testGetElementsReturnsAllIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveElementName1 = "inactiveElement1";
    final String inactiveElementName2 = "inactiveElement2";
    final String inactiveElementName3 = "inactiveElement3";
    final String inactiveElementName4 = "inactiveElement4";


    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );

    IMetaStoreElement inactiveElement1 = getMockElementWithName( inactiveElementName1 );
    IMetaStoreElement inactiveElement2 = getMockElementWithName( inactiveElementName2 );
    IMetaStoreElement inactiveElement3 = getMockElementWithName( inactiveElementName3 );
    IMetaStoreElement inactiveElement4 = getMockElementWithName( inactiveElementName4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    List<IMetaStoreElement> type1Elements = Arrays.asList( inactiveElement1, inactiveElement2, inactiveElement3 );
    when( inactiveMetaStore1.getElementTypeByName( inactiveType1 ) ).thenReturn( inactiveElementType1 );
    when( inactiveMetaStore1.getElements( inactiveElementType1 ) ).thenReturn( type1Elements );

    List<IMetaStoreElement> type2Elements = Arrays.asList( inactiveElement4 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypeByName( inactiveType2 ) ).thenReturn( inactiveElementType2 );
    when( inactiveMetaStore2.getElements( inactiveElementType2 ) ).thenReturn( type2Elements );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, inactiveMetaStore2 );
    List<IMetaStoreElement> elementsType1 = delegatingMetaStore.getElements( inactiveElementType1 );
    List<IMetaStoreElement> elementsType2 = delegatingMetaStore.getElements( inactiveElementType2 );
    assertTrue( containsAll( type1Elements, elementsType1 ) );
    assertFalse( containsAny( type2Elements, elementsType1 ) );
    assertTrue( containsAll( type2Elements, elementsType2 ) );
    assertFalse( containsAny( type1Elements, elementsType2 ) );
  }

  @Test
  public void testGetElementsReturnsOnlyActiveIfMetaStoreIsActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String activeName2 = "active_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveElementName1 = "inactiveElement1";
    final String inactiveElementName2 = "inactiveElement2";
    final String inactiveElementName3 = "inactiveElement3";
    final String inactiveElementName4 = "inactiveElement4";

    

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );

    IMetaStoreElement inactiveElement1 = getMockElementWithName( inactiveElementName1 );
    IMetaStoreElement inactiveElement2 = getMockElementWithName( inactiveElementName2 );
    IMetaStoreElement inactiveElement3 = getMockElementWithName( inactiveElementName3 );
    IMetaStoreElement inactiveElement4 = getMockElementWithName( inactiveElementName4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    List<IMetaStoreElement> type1Elements = Arrays.asList( inactiveElement1, inactiveElement2, inactiveElement3 );
    when( inactiveMetaStore1.getElementTypeByName( inactiveType1 ) ).thenReturn( inactiveElementType1 );
    when( inactiveMetaStore1.getElements( inactiveElementType1 ) ).thenReturn( type1Elements );

    List<IMetaStoreElement> type2Elements = Arrays.asList( inactiveElement4 );
    IMetaStore activeMetaStore2 = getMockMetaStoreWithName( activeName2 );
    when( activeMetaStore2.getElementTypeByName( inactiveType2 ) ).thenReturn( inactiveElementType2 );
    when( activeMetaStore2.getElements( inactiveElementType2 ) ).thenReturn( type2Elements );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName2 );
    List<IMetaStoreElement> elementsType1 = delegatingMetaStore.getElements( inactiveElementType1 );
    List<IMetaStoreElement> elementsType2 = delegatingMetaStore.getElements( inactiveElementType2 );
    assertEquals( 0, elementsType1.size() );
    assertEquals( 1, elementsType2.size() );
    assertTrue( containsAll( type2Elements, elementsType2 ) );
  }

  @Test
  public void testGetElementIdsReturnsAllIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveElementId1 = "inactiveElement1";
    final String inactiveElementId2 = "inactiveElement2";
    final String inactiveElementId3 = "inactiveElement3";
    final String inactiveElementId4 = "inactiveElement4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );

    IMetaStoreElement inactiveElement1 = getMockElementWithIdAndName( inactiveElementId1 );
    IMetaStoreElement inactiveElement2 = getMockElementWithIdAndName( inactiveElementId2 );
    IMetaStoreElement inactiveElement3 = getMockElementWithIdAndName( inactiveElementId3 );
    IMetaStoreElement inactiveElement4 = getMockElementWithIdAndName( inactiveElementId4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    List<IMetaStoreElement> type1Elements = Arrays.asList( inactiveElement1, inactiveElement2, inactiveElement3 );
    when( inactiveMetaStore1.getElementTypeByName( inactiveType1 ) ).thenReturn( inactiveElementType1 );
    when( inactiveMetaStore1.getElements( inactiveElementType1 ) ).thenReturn( type1Elements );

    List<IMetaStoreElement> type2Elements = Arrays.asList( inactiveElement4 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypeByName( inactiveType2 ) ).thenReturn( inactiveElementType2 );
    when( inactiveMetaStore2.getElements( inactiveElementType2 ) ).thenReturn( type2Elements );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, inactiveMetaStore2 );
    List<String> elementsType1 = delegatingMetaStore.getElementIds( inactiveElementType1 );
    List<String> elementsType2 = delegatingMetaStore.getElementIds( inactiveElementType2 );
    assertTrue( containsAll( Arrays.asList( inactiveElementId1, inactiveElementId2, inactiveElementId3 ), elementsType1 ) );
    assertFalse( containsAny( Arrays.asList( inactiveElementId4 ), elementsType1 ) );
    assertTrue( containsAll( Arrays.asList( inactiveElementId4 ), elementsType2 ) );
    assertFalse( containsAny( Arrays.asList( inactiveElementId1, inactiveElementId2, inactiveElementId3 ),
      elementsType2 ) );
  }

  @Test
  public void testGetElementIdsReturnsOnlyActiveIfMetaStoreIsActive() throws MetaStoreException {
    final String activeName1 = "active_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveElementId1 = "inactiveElement1";
    final String inactiveElementId2 = "inactiveElement2";
    final String inactiveElementId3 = "inactiveElement3";
    final String inactiveElementId4 = "inactiveElement4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );

    IMetaStoreElement inactiveElement1 = getMockElementWithIdAndName( inactiveElementId1 );
    IMetaStoreElement inactiveElement2 = getMockElementWithIdAndName( inactiveElementId2 );
    IMetaStoreElement inactiveElement3 = getMockElementWithIdAndName( inactiveElementId3 );
    IMetaStoreElement inactiveElement4 = getMockElementWithIdAndName( inactiveElementId4 );

    IMetaStore activeMetaStore1 = getMockMetaStoreWithName( activeName1 );
    List<IMetaStoreElement> type1Elements = Arrays.asList( inactiveElement1, inactiveElement2, inactiveElement3 );
    when( activeMetaStore1.getElementTypeByName( inactiveType1 ) ).thenReturn( inactiveElementType1 );
    when( activeMetaStore1.getElements( inactiveElementType1 ) ).thenReturn( type1Elements );

    List<IMetaStoreElement> type2Elements = Arrays.asList( inactiveElement4 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypeByName( inactiveType2 ) ).thenReturn( inactiveElementType2 );
    when( inactiveMetaStore2.getElements( inactiveElementType2 ) ).thenReturn( type2Elements );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( activeMetaStore1, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName1 );
    List<String> elementsType1 = delegatingMetaStore.getElementIds( inactiveElementType1 );
    List<String> elementsType2 = delegatingMetaStore.getElementIds( inactiveElementType2 );
    assertTrue( containsAll( Arrays.asList( inactiveElementId1, inactiveElementId2, inactiveElementId3 ), elementsType1 ) );
    assertFalse( containsAny( Arrays.asList( inactiveElementId4 ), elementsType1 ) );
    assertEquals( 0, elementsType2.size() );
  }

  @Test
  public void testGetElementReturnsAllIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveElementName1 = "inactiveElement1";
    final String inactiveElementName2 = "inactiveElement2";
    final String inactiveElementName3 = "inactiveElement3";
    final String inactiveElementName4 = "inactiveElement4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );

    IMetaStoreElement inactiveElement1 = getMockElementWithIdAndName( inactiveElementName1 );
    IMetaStoreElement inactiveElement2 = getMockElementWithIdAndName( inactiveElementName2 );
    IMetaStoreElement inactiveElement3 = getMockElementWithIdAndName( inactiveElementName3 );
    IMetaStoreElement inactiveElement4 = getMockElementWithIdAndName( inactiveElementName4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    List<IMetaStoreElement> type1Elements = Arrays.asList( inactiveElement1, inactiveElement2, inactiveElement3 );
    when( inactiveMetaStore1.getElementTypeByName( inactiveType1 ) ).thenReturn( inactiveElementType1 );
    when( inactiveMetaStore1.getElements( inactiveElementType1 ) ).thenReturn( type1Elements );

    List<IMetaStoreElement> type2Elements = Arrays.asList( inactiveElement4 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypeByName( inactiveType2 ) ).thenReturn( inactiveElementType2 );
    when( inactiveMetaStore2.getElements( inactiveElementType2 ) ).thenReturn( type2Elements );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, inactiveMetaStore2 );
    assertEquals( inactiveElement1, delegatingMetaStore.getElement( inactiveElementType1,
      inactiveElementName1 ) );
    assertEquals( inactiveElement2, delegatingMetaStore.getElement( inactiveElementType1,
      inactiveElementName2 ) );
    assertEquals( inactiveElement3, delegatingMetaStore.getElement( inactiveElementType1,
      inactiveElementName3 ) );
    assertEquals( inactiveElement4, delegatingMetaStore.getElement( inactiveElementType2,
      inactiveElementName4 ) );
  }

  @Test
  public void testGetElementReturnsOnlyActiveIfMetaStoreIsActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveElementName1 = "inactiveElement1";
    final String inactiveElementName2 = "inactiveElement2";
    final String inactiveElementName3 = "inactiveElement3";
    final String inactiveElementName4 = "inactiveElement4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );

    IMetaStoreElement inactiveElement1 = getMockElementWithIdAndName( inactiveElementName1 );
    IMetaStoreElement inactiveElement2 = getMockElementWithIdAndName( inactiveElementName2 );
    IMetaStoreElement inactiveElement3 = getMockElementWithIdAndName( inactiveElementName3 );
    IMetaStoreElement inactiveElement4 = getMockElementWithIdAndName( inactiveElementName4 );

    IMetaStore activeMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    List<IMetaStoreElement> type1Elements = Arrays.asList( inactiveElement1, inactiveElement2, inactiveElement3 );
    when( activeMetaStore1.getElementTypeByName( inactiveType1 ) ).thenReturn( inactiveElementType1 );
    when( activeMetaStore1.getElements( inactiveElementType1 ) ).thenReturn( type1Elements );

    List<IMetaStoreElement> type2Elements = Arrays.asList( inactiveElement4 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypeByName( inactiveType2 ) ).thenReturn( inactiveElementType2 );
    when( inactiveMetaStore2.getElements( inactiveElementType2 ) ).thenReturn( type2Elements );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( activeMetaStore1, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeMetaStore1.getName() );
    assertEquals( inactiveElement1, delegatingMetaStore.getElement( inactiveElementType1,
      inactiveElementName1 ) );
    assertEquals( inactiveElement2, delegatingMetaStore.getElement( inactiveElementType1,
      inactiveElementName2 ) );
    assertEquals( inactiveElement3, delegatingMetaStore.getElement( inactiveElementType1,
      inactiveElementName3 ) );
    assertNull( delegatingMetaStore.getElement( inactiveElementType2, inactiveElementName4 ) );
  }

  @Test
  public void testGetElementByNameReturnsAllIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveElementName1 = "inactiveElement1";
    final String inactiveElementName2 = "inactiveElement2";
    final String inactiveElementName3 = "inactiveElement3";
    final String inactiveElementName4 = "inactiveElement4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );

    IMetaStoreElement inactiveElement1 = getMockElementWithName( inactiveElementName1 );
    IMetaStoreElement inactiveElement2 = getMockElementWithName( inactiveElementName2 );
    IMetaStoreElement inactiveElement3 = getMockElementWithName( inactiveElementName3 );
    IMetaStoreElement inactiveElement4 = getMockElementWithName( inactiveElementName4 );

    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    List<IMetaStoreElement> type1Elements = Arrays.asList( inactiveElement1, inactiveElement2, inactiveElement3 );
    when( inactiveMetaStore1.getElementTypeByName( inactiveType1 ) ).thenReturn( inactiveElementType1 );
    when( inactiveMetaStore1.getElements( inactiveElementType1 ) ).thenReturn( type1Elements );

    List<IMetaStoreElement> type2Elements = Arrays.asList( inactiveElement4 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypeByName( inactiveType2 ) ).thenReturn( inactiveElementType2 );
    when( inactiveMetaStore2.getElements( inactiveElementType2 ) ).thenReturn( type2Elements );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, inactiveMetaStore2 );
    assertEquals( inactiveElement1, delegatingMetaStore.getElementByName( inactiveElementType1, inactiveElementName1 ) );
    assertEquals( inactiveElement2, delegatingMetaStore.getElementByName( inactiveElementType1, inactiveElementName2 ) );
    assertEquals( inactiveElement3, delegatingMetaStore.getElementByName( inactiveElementType1, inactiveElementName3 ) );
    assertEquals( inactiveElement4, delegatingMetaStore.getElementByName( inactiveElementType2, inactiveElementName4 ) );
  }

  @Test
  public void testGetElementByNameReturnsOnlyActiveIfMetaStoreIsActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String inactiveType1 = "inactiveType1";
    final String inactiveType2 = "inactiveType2";
    final String inactiveElementName1 = "inactiveElement1";
    final String inactiveElementName2 = "inactiveElement2";
    final String inactiveElementName3 = "inactiveElement3";
    final String inactiveElementName4 = "inactiveElement4";

    IMetaStoreElementType inactiveElementType1 = getMockElementTypeWithName( inactiveType1 );
    IMetaStoreElementType inactiveElementType2 = getMockElementTypeWithName( inactiveType2 );

    IMetaStoreElement inactiveElement1 = getMockElementWithName( inactiveElementName1 );
    IMetaStoreElement inactiveElement2 = getMockElementWithName( inactiveElementName2 );
    IMetaStoreElement inactiveElement3 = getMockElementWithName( inactiveElementName3 );
    IMetaStoreElement inactiveElement4 = getMockElementWithName( inactiveElementName4 );

    IMetaStore activeMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    List<IMetaStoreElement> type1Elements = Arrays.asList( inactiveElement1, inactiveElement2, inactiveElement3 );
    when( activeMetaStore1.getElementTypeByName( inactiveType1 ) ).thenReturn( inactiveElementType1 );
    when( activeMetaStore1.getElements( inactiveElementType1 ) ).thenReturn( type1Elements );

    List<IMetaStoreElement> type2Elements = Arrays.asList( inactiveElement4 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    when( inactiveMetaStore2.getElementTypeByName( inactiveType2 ) ).thenReturn( inactiveElementType2 );
    when( inactiveMetaStore2.getElements( inactiveElementType2 ) ).thenReturn( type2Elements );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( activeMetaStore1, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeMetaStore1.getName() );
    assertEquals( inactiveElement1, delegatingMetaStore.getElementByName( inactiveElementType1, inactiveElementName1 ) );
    assertEquals( inactiveElement2, delegatingMetaStore.getElementByName( inactiveElementType1, inactiveElementName2 ) );
    assertEquals( inactiveElement3, delegatingMetaStore.getElementByName( inactiveElementType1, inactiveElementName3 ) );
    assertNull( delegatingMetaStore.getElementByName( inactiveElementType2, inactiveElementName4 ) );
  }

  @Test( expected = MetaStoreException.class )
  public void testCreateElementThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );
    IMetaStoreElement mockElement = mock( IMetaStoreElement.class );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.createElement( mockElementType, mockElement );
  }

  @Test
  public void testCreateElementOnlyCreatesElementTypeInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );
    IMetaStoreElement mockElement = mock( IMetaStoreElement.class );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.createElement( mockElementType, mockElement );
    verify( activeMetaStore ).createElement( mockElementType, mockElement );
    verify( inactiveMetaStore1, never() ).createElement( any( IMetaStoreElementType.class ), any( IMetaStoreElement.class ) );
    verify( inactiveMetaStore2, never() ).createElement( any( IMetaStoreElementType.class ), any( IMetaStoreElement.class ) );
  }

  @Test( expected = MetaStoreException.class )
  public void testDeleteElementThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    
    final String mockElementId = "mockid";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.deleteElement( mockElementType, mockElementId );
  }

  @Test
  public void testDeleteElementOnlyDeletesElementTypeInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";
    
    final String mockElementId = "mockid";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.deleteElement( mockElementType, mockElementId );
    verify( activeMetaStore ).deleteElement( mockElementType, mockElementId );
    verify( inactiveMetaStore1, never() ).deleteElement( any( IMetaStoreElementType.class ), anyString() );
    verify( inactiveMetaStore2, never() ).deleteElement( any( IMetaStoreElementType.class ), anyString() );
  }

  @Test( expected = MetaStoreException.class )
  public void testUpdateElementThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    
    final String mockElementId = "mockid";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );
    IMetaStoreElement mockElement = getMockElementWithName( mockElementId );

    DelegatingMetaStore delegatingMetaStore = new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.updateElement( mockElementType, mockElementId, mockElement );
  }

  @Test
  public void testUpdateElementOnlyUpdatesElementTypeInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";
    
    final String mockElementId = "mockid";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );
    IMetaStoreElement mockElement = getMockElementWithName( mockElementId );

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.updateElement( mockElementType, mockElementId, mockElement );
    verify( activeMetaStore ).updateElement( mockElementType, mockElementId, mockElement );
    verify( inactiveMetaStore1, never() ).updateElement( any( IMetaStoreElementType.class ), anyString(), any( IMetaStoreElement.class ) );
    verify( inactiveMetaStore2, never() ).updateElement( any( IMetaStoreElementType.class ), anyString(), any( IMetaStoreElement.class ) );
  }

  @Test( expected = MetaStoreException.class )
  public void testNewElementThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.newElement();
  }

  @Test
  public void testNewElementOnlyCreatesNewElementInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.newElement();
    verify( activeMetaStore ).newElement();
    verify( inactiveMetaStore1, never() ).newElement();
    verify( inactiveMetaStore2, never() ).newElement();
  }

  @Test( expected = MetaStoreException.class )
  public void testNewElementWithArgsThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String mockElementId = "mockid";

    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );
    Object value = new Object();

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.newElement( mockElementType, mockElementId, value );
  }

  @Test
  public void testNewElementWithArgsOnlyCreatesNewElementInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";
    final String mockElementId = "mockid";

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    IMetaStoreElementType mockElementType = mock( IMetaStoreElementType.class );
    Object value = new Object();

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.newElement( mockElementType, mockElementId, value );
    verify( activeMetaStore ).newElement( mockElementType, mockElementId, value );
    verify( inactiveMetaStore1, never() ).newElement( any( IMetaStoreElementType.class ), anyString(),
      any( Object.class ) );
    verify( inactiveMetaStore2, never() ).newElement( any( IMetaStoreElementType.class ), anyString(),
      any( Object.class ) );
  }

  @Test( expected = MetaStoreException.class )
  public void testNewAttributeThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String mockElementId = "mockid";

    Object value = new Object();

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.newAttribute( mockElementId, value );
  }

  @Test
  public void testNewAttributeOnlyCreatesNewElementInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";
    final String mockElementId = "mockid";

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    Object value = new Object();

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.newAttribute( mockElementId, value );
    verify( activeMetaStore ).newAttribute( mockElementId, value );
    verify( inactiveMetaStore1, never() ).newAttribute( anyString(), any( Object.class ) );
    verify( inactiveMetaStore2, never() ).newAttribute( anyString(), any( Object.class ) );
  }

  @Test( expected = MetaStoreException.class )
  public void testNewElementOwnerThrowsExceptionIfNoActive() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String mockElementId = "mockid";

    MetaStoreElementOwnerType ownerType = MetaStoreElementOwnerType.ROLE;

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( getMockMetaStoreWithName( inactiveName1 ), getMockMetaStoreWithName( inactiveName2 ) );
    delegatingMetaStore.newElementOwner( mockElementId, ownerType );
  }

  @Test
  public void testNewElementOwnerOnlyCreatesNewElementInActiveMetaStore() throws MetaStoreException {
    final String inactiveName1 = "inactive_1";
    final String inactiveName2 = "inactive_2";
    final String activeName = "active";
    final String mockElementId = "mockid";

    IMetaStore activeMetaStore = getMockMetaStoreWithName( activeName );
    IMetaStore inactiveMetaStore1 = getMockMetaStoreWithName( inactiveName1 );
    IMetaStore inactiveMetaStore2 = getMockMetaStoreWithName( inactiveName2 );
    MetaStoreElementOwnerType ownerType = MetaStoreElementOwnerType.ROLE;

    DelegatingMetaStore delegatingMetaStore =
      new DelegatingMetaStore( inactiveMetaStore1, activeMetaStore, inactiveMetaStore2 );
    delegatingMetaStore.setActiveMetaStoreName( activeName );
    delegatingMetaStore.newElementOwner( mockElementId, ownerType );
    verify( activeMetaStore ).newElementOwner( mockElementId, ownerType );
    verify( inactiveMetaStore1, never() ).newAttribute( anyString(), any( MetaStoreElementOwnerType.class ) );
    verify( inactiveMetaStore2, never() ).newAttribute( anyString(), any( MetaStoreElementOwnerType.class ) );
  }

  private IMetaStore getMockMetaStoreWithName( String name ) throws MetaStoreException {
    IMetaStore result = mock( IMetaStore.class );
    when( result.getName() ).thenReturn( name );
    return result;
  }

  private IMetaStoreElementType getMockElementTypeWithName( String name ) throws MetaStoreException {
    IMetaStoreElementType result = mock( IMetaStoreElementType.class );
    when( result.getName() ).thenReturn( name );
    return result;
  }

  private IMetaStoreElementType getMockElementTypeWithIdAndName( String id ) throws MetaStoreException {
    IMetaStoreElementType result = mock( IMetaStoreElementType.class );
    when( result.getName() ).thenReturn( id + "-name" );
    when( result.getId() ).thenReturn( id );
    return result;
  }

  private IMetaStoreElement getMockElementWithName( String name ) {
    IMetaStoreElement result = mock( IMetaStoreElement.class );
    when( result.getName() ).thenReturn( name );
    return result;
  }

  private IMetaStoreElement getMockElementWithIdAndName( String id ) throws MetaStoreException {
    IMetaStoreElement result = mock( IMetaStoreElement.class );
    when( result.getName() ).thenReturn( id + "-name" );
    when( result.getId() ).thenReturn( id );
    return result;
  }

  private <T> boolean containsAll( Collection<T> expected, Collection<T> actual ) {
    for ( T expectedItem : expected ) {
      if ( !actual.contains( expectedItem ) ) {
        return false;
      }
    }
    return true;
  }

  private <T> boolean containsAny( Collection<T> expected, Collection<T> actual ) {
    for ( T expectedItem : expected ) {
      if ( actual.contains( expectedItem ) ) {
        return true;
      }
    }
    return false;
  }
}
