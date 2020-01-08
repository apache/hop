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

import junit.framework.TestCase;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementTypeExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.MetaStoreElementOwnerType;
import org.apache.hop.metastore.api.security.MetaStoreObjectPermission;
import org.apache.hop.metastore.api.security.MetaStoreOwnerPermissions;
import org.apache.hop.metastore.util.HopDefaults;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Ignore
public class MetaStoreTestBase extends TestCase {
  // MetaStore Name
  protected static String META_STORE_NAME = "TestMetaStore";

  // Namespace: Hitachi Vantara
  //
  protected static String namespace = HopDefaults.NAMESPACE;

  // Element type: Shared Dimension
  //
  protected static final String SHARED_DIMENSION_NAME = "Shared Dimension";
  protected static final String SHARED_DIMENSION_DESCRIPTION = "Star modeler shared dimension";

  // Element: customer dimension
  //
  protected static final String CUSTOMER_DIMENSION_NAME = "Customer dimension";

  public void testFunctionality( IMetaStore metaStore ) throws MetaStoreException {
    if ( !metaStore.namespaceExists( namespace ) ) {
      metaStore.createNamespace( namespace );
    }
    List<String> namespaces = metaStore.getNamespaces();
    assertEquals( 1, namespaces.size() );

    IMetaStoreElementType elementType = metaStore.newElementType( namespace );
    elementType.setName( SHARED_DIMENSION_NAME );
    elementType.setDescription( SHARED_DIMENSION_DESCRIPTION );
    metaStore.createElementType( namespace, elementType );
    assertNotNull( elementType.getId() );

    List<IMetaStoreElementType> elementTypes = metaStore.getElementTypes( namespace );
    assertEquals( 1, elementTypes.size() );

    try {

      metaStore.createElementType( namespace, elementType );
      fail( "Duplicate creation error expected!" );
    } catch ( MetaStoreElementTypeExistsException e ) {
      // OK!
    } catch ( MetaStoreException e ) {
      e.printStackTrace();
      fail( "Create exception needs to be MetaStoreDataTypesExistException" );
    }

    // Try to delete the namespace, should error out
    //
    try {
      metaStore.deleteNamespace( namespace );
      fail( "Expected error while deleting namespace with content!" );
    } catch ( MetaStoreDependenciesExistsException e ) {
      // OK!
      List<String> dependencies = e.getDependencies();
      assertNotNull( dependencies );
      assertEquals( 1, dependencies.size() );
      assertEquals( elementType.getId(), dependencies.get( 0 ) );
    }

    IMetaStoreElement customerDimension = generateCustomerDimensionElement( metaStore, elementType );
    IMetaStoreElementOwner elementOwner = customerDimension.getOwner();
    assertNotNull( elementOwner );
    assertEquals( "joe", elementOwner.getName() );
    assertEquals( MetaStoreElementOwnerType.USER, elementOwner.getOwnerType() );

    metaStore.createElement( namespace, elementType, customerDimension );
    assertNotNull( customerDimension.getId() );
    List<IMetaStoreElement> elements = metaStore.getElements( namespace, elementType );
    assertEquals( 1, elements.size() );
    assertNotNull( elements.get( 0 ) );
    assertEquals( CUSTOMER_DIMENSION_NAME, elements.get( 0 ).getName() );

    // Try to delete the data type, should error out
    //
    try {
      metaStore.deleteElementType( namespace, elementType );
      fail( "Expected error while deleting data type with content!" );
    } catch ( MetaStoreDependenciesExistsException e ) {
      // OK!
      List<String> dependencies = e.getDependencies();
      assertNotNull( dependencies );
      assertEquals( 1, dependencies.size() );
      assertEquals( customerDimension.getId(), dependencies.get( 0 ) );
    }

    // Some lookup-by-name tests...
    //
    assertNotNull( metaStore.getElementTypeByName( namespace, SHARED_DIMENSION_NAME ) );
    assertNotNull( metaStore.getElementByName( namespace, elementType, CUSTOMER_DIMENSION_NAME ) );

    assertNotNull( metaStore.getElement( namespace, elementType, CUSTOMER_DIMENSION_NAME ) );
    assertEquals( 1, metaStore.getElementIds( namespace, elementType ).size() );
    assertEquals( SHARED_DIMENSION_NAME, metaStore.getElementType( namespace, SHARED_DIMENSION_NAME ).getId() );
    assertEquals( 1, metaStore.getElementTypeIds( namespace ).size() );
    assertEquals( 1, metaStore.getElementTypes( namespace ).size() );

    // Some update tests...
    customerDimension.setValue( SHARED_DIMENSION_NAME );
    metaStore.updateElement( namespace, elementType, CUSTOMER_DIMENSION_NAME, customerDimension );
    assertNotNull( metaStore.getElementByName( namespace, elementType, CUSTOMER_DIMENSION_NAME ).getValue() );

    elementType.setDescription( CUSTOMER_DIMENSION_NAME );
    metaStore.updateElementType( namespace, elementType );
    assertNotNull( metaStore.getElementTypeByName( namespace, elementType.getName() ).getDescription() );

    // Clean up shop!
    //
    metaStore.deleteElement( namespace, elementType, customerDimension.getId() );
    elements = metaStore.getElements( namespace, elementType );
    assertEquals( 0, elements.size() );

    metaStore.deleteElementType( namespace, elementType );
    elementTypes = metaStore.getElementTypes( namespace );
    assertEquals( 0, elementTypes.size() );

    metaStore.deleteNamespace( namespace );
    namespaces = metaStore.getNamespaces();
    assertEquals( 0, namespaces.size() );
  }

  private IMetaStoreElement generateCustomerDimensionElement( IMetaStore metaStore, IMetaStoreElementType elementType )
    throws MetaStoreException {
    IMetaStoreElement element = metaStore.newElement();
    element.setElementType( elementType );
    element.setName( CUSTOMER_DIMENSION_NAME );

    element.addChild( metaStore.newAttribute( "description", "This is the shared customer dimension" ) );
    element.addChild( metaStore.newAttribute( "physical_table", "DIM_CUSTOMER" ) );
    IMetaStoreAttribute fieldsElement = metaStore.newAttribute( "fields", null );
    element.addChild( fieldsElement );

    // A technical key
    //
    IMetaStoreAttribute fieldElement = metaStore.newAttribute( "field_0", null );
    fieldsElement.addChild( fieldElement );
    fieldElement.addChild( metaStore.newAttribute( "field_name", "Customer TK" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_description", "Customer Technical key" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_phyiscal_name", "customer_tk" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_kettle_type", "Integer" ) );

    // A version field
    //
    fieldElement = metaStore.newAttribute( "field_1", null );
    fieldsElement.addChild( fieldElement );
    fieldElement.addChild( metaStore.newAttribute( "field_name", "version field" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_description", "dimension version field (1..N)" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_phyiscal_name", "version" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_kettle_type", "Integer" ) );

    // Natural key
    //
    fieldElement = metaStore.newAttribute( "field_2", null );
    fieldsElement.addChild( fieldElement );
    fieldElement.addChild( metaStore.newAttribute( "field_name", "Customer ID" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_description",
      "Customer ID as a natural key of this dimension" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_phyiscal_name", "customer_id" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_kettle_type", "Integer" ) );

    // Start date
    //
    fieldElement = metaStore.newAttribute( "field_3", null );
    fieldsElement.addChild( fieldElement );
    fieldElement.addChild( metaStore.newAttribute( "field_name", "Start date" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_description", "Start of validity of this dimension entry" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_phyiscal_name", "start_date" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_kettle_type", "Date" ) );

    // End date
    //
    fieldElement = metaStore.newAttribute( "field_4", null );
    fieldsElement.addChild( fieldElement );
    fieldElement.addChild( metaStore.newAttribute( "field_name", "End date" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_description", "End of validity of this dimension entry" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_phyiscal_name", "end_date" ) );
    fieldElement.addChild( metaStore.newAttribute( "field_kettle_type", "Date" ) );

    // A few columns...
    //
    for ( int i = 5; i <= 10; i++ ) {
      fieldElement = metaStore.newAttribute( "field_" + i, null );
      fieldsElement.addChild( fieldElement );
      fieldElement.addChild( metaStore.newAttribute( "field_name", "Field name " + i ) );
      fieldElement.addChild( metaStore.newAttribute( "field_description", "Field description " + i ) );
      fieldElement.addChild( metaStore.newAttribute( "field_phyiscal_name", "physical_name_" + i ) );
      fieldElement.addChild( metaStore.newAttribute( "field_kettle_type", "String" ) );
    }

    // Some security
    //
    element.setOwner( metaStore.newElementOwner( "joe", MetaStoreElementOwnerType.USER ) );

    // The "users" role has read/write permissions
    //
    IMetaStoreElementOwner usersRole = metaStore.newElementOwner( "users", MetaStoreElementOwnerType.ROLE );
    MetaStoreOwnerPermissions usersRoleOwnerPermissions =
      new MetaStoreOwnerPermissions( usersRole, MetaStoreObjectPermission.READ, MetaStoreObjectPermission.UPDATE );
    element.getOwnerPermissionsList().add( usersRoleOwnerPermissions );

    return element;
  }

  public void testParallelOneStore( final IMetaStore metaStore ) throws Exception {

    final List<Exception> exceptions = new ArrayList<Exception>();
    try {
      List<Thread> threads = new ArrayList<Thread>();

      for ( int i = 9000; i < 9020; i++ ) {
        final int index = i;
        Thread thread = new Thread() {
          public void run() {
            try {
              parallelStoreRetrieve( metaStore, index );
            } catch ( Exception e ) {
              exceptions.add( e );
            }
          }
        };
        threads.add( thread );
        thread.start();
      }

      for ( Thread thread : threads ) {
        thread.join();
      }
    } finally {

    }

    if ( !exceptions.isEmpty() ) {
      fail( exceptions.size() + " exceptions encountered during parallel store/retrieve" );
      for ( Exception e : exceptions ) {
        e.printStackTrace( System.err );
      }
    }
  }

  protected void parallelStoreRetrieve( final IMetaStore metaStore, final int index ) throws MetaStoreException {
    String namespace = "ns-" + index;
    metaStore.createNamespace( namespace );

    int nrTypes = 5;
    int nrElements = 20;

    for ( int typeNr = 50; typeNr < 50 + nrTypes; typeNr++ ) {
      IMetaStoreElementType elementType = metaStore.newElementType( namespace );
      String typeName = "type-name-" + index + "-" + typeNr;
      String typeDescription = "type-description-" + index + "-" + typeNr;
      elementType.setName( typeName );
      elementType.setDescription( typeDescription );
      metaStore.createElementType( namespace, elementType );

      assertNotNull( elementType.getId() );

      IMetaStoreElementType verifyType = metaStore.getElementType( namespace, elementType.getId() );
      assertEquals( typeName, verifyType.getName() );
      assertEquals( typeDescription, verifyType.getDescription() );
      assertNotNull( verifyType.getId() );

      verifyType = metaStore.getElementTypeByName( namespace, elementType.getName() );
      assertEquals( typeName, verifyType.getName() );
      assertEquals( typeDescription, verifyType.getDescription() );
      assertNotNull( verifyType.getId() );

      // Populate
      List<IMetaStoreElement> elements = new ArrayList<IMetaStoreElement>();
      for ( int i = 100; i < 100 + nrElements; i++ ) {
        IMetaStoreElement element = populateElement( metaStore, "element-" + index + "-" + i );
        elements.add( element );
        metaStore.createElement( namespace, elementType, element );
        assertNotNull( element.getId() );
      }

      try {
        metaStore.deleteElementType( namespace, elementType );
        fail( "Unable to detect dependencies" );
      } catch ( MetaStoreDependenciesExistsException e ) {
        // OK
        assertEquals( e.getDependencies().size(), elements.size() );
      }
    }

    for ( int typeNr = 50; typeNr < 50 + nrTypes; typeNr++ ) {
      String typeName = "type-name-" + index + "-" + typeNr;
      IMetaStoreElementType elementType = metaStore.getElementTypeByName( namespace, typeName );
      assertNotNull( elementType );

      List<IMetaStoreElement> verifyElements = metaStore.getElements( namespace, elementType );
      assertEquals( nrElements, verifyElements.size() );

      // the elements come back in an unpredictable order
      // sort by name
      //
      Collections.sort( verifyElements, new Comparator<IMetaStoreElement>() {
        @Override
        public int compare( IMetaStoreElement o1, IMetaStoreElement o2 ) {
          return o1.getName().compareTo( o2.getName() );
        }
      } );

      // Validate
      for ( int i = 0; i < verifyElements.size(); i++ ) {
        IMetaStoreElement element = verifyElements.get( i );
        validateElement( element, "element-" + index + "-" + ( 100 + i ) );
        metaStore.deleteElement( namespace, elementType, element.getId() );
      }

      verifyElements = metaStore.getElements( namespace, elementType );
      assertEquals( 0, verifyElements.size() );

      metaStore.deleteElementType( namespace, elementType );
    }
  }

  protected IMetaStoreElement populateElement( IMetaStore metaStore, String name ) throws MetaStoreException {
    IMetaStoreElement element = metaStore.newElement();
    element.setName( name );
    for ( int i = 1; i <= 5; i++ ) {
      element.addChild( metaStore.newAttribute( "id " + i, "value " + i ) );
    }
    IMetaStoreAttribute subAttr = metaStore.newAttribute( "sub-attr", null );
    for ( int i = 101; i <= 110; i++ ) {
      subAttr.addChild( metaStore.newAttribute( "sub-id " + i, "sub-value " + i ) );
    }
    element.addChild( subAttr );

    return element;
  }

  protected void validateElement( IMetaStoreElement element, String name ) throws MetaStoreException {
    assertEquals( name, element.getName() );
    assertEquals( 6, element.getChildren().size() );
    for ( int i = 1; i <= 5; i++ ) {
      IMetaStoreAttribute child = element.getChild( "id " + i );
      assertEquals( "value " + i, child.getValue() );
    }
    IMetaStoreAttribute subAttr = element.getChild( "sub-attr" );
    assertNotNull( subAttr );
    assertEquals( 10, subAttr.getChildren().size() );
    for ( int i = 101; i <= 110; i++ ) {
      IMetaStoreAttribute child = subAttr.getChild( "sub-id " + i );
      assertNotNull( child );
      assertEquals( "sub-value " + i, child.getValue() );
    }
  }

}
