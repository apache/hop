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

import junit.framework.TestCase;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.IMetaStoreObjectFactory;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.metastore.test.testclasses.cube.Cube;
import org.apache.hop.metastore.test.testclasses.cube.Dimension;
import org.apache.hop.metastore.test.testclasses.cube.DimensionAttribute;
import org.apache.hop.metastore.test.testclasses.cube.DimensionType;
import org.apache.hop.metastore.test.testclasses.cube.Kpi;
import org.apache.hop.metastore.test.testclasses.factory.A;
import org.apache.hop.metastore.test.testclasses.factory.B;
import org.apache.hop.metastore.test.testclasses.factory_shared.X;
import org.apache.hop.metastore.test.testclasses.factory_shared.Y;
import org.apache.hop.metastore.test.testclasses.my.MyElement;
import org.apache.hop.metastore.test.testclasses.my.MyElementAttr;
import org.apache.hop.metastore.test.testclasses.my.MyFilenameElement;
import org.apache.hop.metastore.test.testclasses.my.MyMigrationElement;
import org.apache.hop.metastore.test.testclasses.my.MyNameElement;
import org.apache.hop.metastore.test.testclasses.my.MyOtherElement;
import org.apache.hop.metastore.util.MetaStoreUtil;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetaStoreFactoryTest extends TestCase {

  public static final String PASSWORD = "my secret password";
  public static final String ANOTHER = "2222222";
  public static final String ATTR = "11111111";
  public static final String NAME = "one";
  public static final int INT = 3;
  public static final long LONG = 4;
  public static final boolean BOOL = true;
  public static final Date DATE = new Date();
  public static final int NR_ATTR = 10;
  public static final int NR_NAME = 5;
  public static final int NR_FILENAME = 5;

  @Test
  public void testIDMigration() throws Exception {

    String namespace = "custom";
    String pipelineName = "Pipeline Name";
    String transformName = "Transform Name";
    String elementName = "migration";
    String hostName = "Host Name";
    String fieldMappings = "Field Mappings";
    String sourceFieldName = "Source Field Name";
    String targetFieldName = "Target Field Name";
    String parameterName = "Parameter Name";

    IMetaStore metaStore = new MemoryMetaStore();

    MetaStoreFactory<MyMigrationElement> factory =
      new MetaStoreFactory<MyMigrationElement>( MyMigrationElement.class, metaStore, namespace );

    if ( !metaStore.namespaceExists( namespace ) ) {
      metaStore.createNamespace( namespace );
    }

    MetaStoreElementType elementTypeAnnotation = MyMigrationElement.class.getAnnotation( MetaStoreElementType.class );

    // Make sure the element type exists...
    IMetaStoreElementType elementType = metaStore.getElementTypeByName( namespace, elementTypeAnnotation.name() );
    if ( elementType == null ) {
      elementType = metaStore.newElementType( namespace );
      elementType.setName( elementTypeAnnotation.name() );
      elementType.setDescription( elementTypeAnnotation.description() );
      metaStore.createElementType( namespace, elementType );
    }

    // Create an element with the old keys we want to migrate
    IMetaStoreElement element = metaStore.newElement();
    element.setName( elementName );
    element.setElementType( elementType );

    element.addChild( metaStore.newAttribute( MyMigrationElement.MY_MIGRATION_PIPELINE_NAME, pipelineName ) );
    element.addChild( metaStore.newAttribute( MyMigrationElement.MY_MIGRATION_TRANSFORM_NAME, transformName ) );
    element.addChild( metaStore.newAttribute( MyMigrationElement.MY_MIGRATION_HOST_NAME, hostName ) );
    element.addChild( metaStore.newAttribute( MyMigrationElement.MY_MIGRATION_FIELD_MAPPINGS, fieldMappings ) );
    element.addChild( metaStore.newAttribute( MyMigrationElement.MY_MIGRATION_SOURCE_FIELD_NAME, sourceFieldName ) );
    element.addChild( metaStore.newAttribute( MyMigrationElement.MY_MIGRATION_TARGET_FIELD_NAME, targetFieldName ) );
    element.addChild( metaStore.newAttribute( MyMigrationElement.MY_MIGRATION_PARAMETER_NAME, parameterName ) );

    metaStore.createElement( namespace, elementType, element );

    MyMigrationElement loadedElement = factory.loadElement( elementName );

    assertNotNull( loadedElement );
    assertEquals( loadedElement.getPipelineName(), pipelineName );
    assertEquals( loadedElement.getTransformName(), transformName );
    assertEquals( loadedElement.getHostname(), hostName );
    assertEquals( loadedElement.getFieldMappings(), fieldMappings );
    assertEquals( loadedElement.getSourceFieldName(), sourceFieldName );
    assertEquals( loadedElement.getTargetFieldName(), targetFieldName );
    assertEquals( loadedElement.getParameterName(), parameterName );

    // Test the variation of the transform name id
    element = metaStore.newElement();
    element.setName( elementName );
    element.setElementType( elementType );

    element.addChild( metaStore.newAttribute( MyMigrationElement.MY_MIGRATION_TRANSFORM_NAME, transformName ) );

    metaStore.createElement( namespace, elementType, element );

    loadedElement = factory.loadElement( elementName );

    assertNotNull( loadedElement );
    assertEquals( loadedElement.getTransformName(), transformName );
  }

  @Test
  public void testMyElement() throws Exception {

    IMetaStore metaStore = new MemoryMetaStore();

    // List of named elements...
    //
    List<MyNameElement> nameList = new ArrayList<MyNameElement>();
    for ( int i = 0; i < NR_NAME; i++ ) {
      nameList.add( new MyNameElement( "name" + i, "description" + i, "color" + i ) );
    }
    List<MyFilenameElement> filenameList = new ArrayList<MyFilenameElement>();
    for ( int i = 0; i < NR_FILENAME; i++ ) {
      filenameList.add( new MyFilenameElement( "filename" + i, "size" + i, "gender" + i ) );
    }

    // Construct our test element...
    //
    MyElement me = new MyElement( NAME, ATTR, ANOTHER, PASSWORD, INT, LONG, BOOL, DATE );
    for ( int i = 0; i < NR_ATTR; i++ ) {
      me.getSubAttributes().add( new MyElementAttr( "key" + i, "value" + i, "desc" + i ) );
    }
    me.setNameElement( nameList.get( NR_NAME - 1 ) );
    me.setFilenameElement( filenameList.get( NR_FILENAME - 1 ) );
    List<String> stringList = Arrays.asList( "a", "b", "c", "d" );
    me.setStringList( stringList );
    MyOtherElement myOtherElement = new MyOtherElement( "other", "other attribute" );
    me.setMyOtherElement( myOtherElement );

    MetaStoreFactory<MyOtherElement> otherFactory = new MetaStoreFactory<MyOtherElement>( MyOtherElement.class, metaStore, "custom" );
    MetaStoreFactory<MyElement> factory = new MetaStoreFactory<MyElement>( MyElement.class, metaStore, "custom" );

    // For loading, specify the name, filename lists or factory that we're referencing...
    //
    factory.addNameList( MyElement.LIST_KEY_MY_NAMES, nameList );
    factory.addFilenameList( MyElement.LIST_KEY_MY_FILENAMES, filenameList );
    factory.addNameFactory( MyElement.FACTORY_OTHER_ELEMENT, otherFactory );

    // Store the class in the meta store
    //
    factory.saveElement( me );

    // Load the class from the meta store
    //
    MyElement verify = factory.loadElement( NAME );

    // Verify list element details...
    //
    IMetaStoreElement element = metaStore.getElementByName( "custom", factory.getElementType(), NAME );
    assertNotNull( element );

    // Verify the general idea
    //
    assertNotNull( verify );
    assertEquals( ATTR, verify.getMyAttribute() );
    assertEquals( ANOTHER, verify.getAnotherAttribute() );
    assertEquals( PASSWORD, verify.getPasswordAttribute() );
    assertEquals( INT, verify.getIntAttribute() );
    assertEquals( LONG, verify.getLongAttribute() );
    assertEquals( BOOL, verify.isBoolAttribute() );
    assertEquals( DATE, verify.getDateAttribute() );
    assertEquals( me.getSubAttributes().size(), verify.getSubAttributes().size() );
    assertEquals( me.getNameElement(), verify.getNameElement() );
    assertEquals( me.getFilenameElement(), verify.getFilenameElement() );

    // verify the details...
    //
    assertTrue( metaStore.namespaceExists( "custom" ) );
    IMetaStoreElementType elementType = factory.getElementType();
    assertNotNull( elementType );
    assertEquals( "My element type", elementType.getName() );
    assertEquals( "This is my element type", elementType.getDescription() );

    assertNotNull( element );
    IMetaStoreAttribute child = element.getChild( "my_attribute" );
    assertNotNull( child );
    assertEquals( ATTR, MetaStoreUtil.getAttributeString( child ) );
    child = element.getChild( "passwordAttribute" );
    assertNotNull( child );
    assertNotSame( "Password needs to be encoded", PASSWORD, MetaStoreUtil.getAttributeString( child ) );

    child = element.getChild( "anotherAttribute" );
    assertNotNull( child );
    assertEquals( ANOTHER, MetaStoreUtil.getAttributeString( child ) );

    // Verify the child attributes as well...
    // This also verifies that the attributes are in the right order.
    // The list can't be re-ordered after loading.
    //
    for ( int i = 0; i < NR_ATTR; i++ ) {
      MyElementAttr attr = verify.getSubAttributes().get( i );
      assertEquals( "key" + i, attr.getKey() );
      assertEquals( "value" + i, attr.getValue() );
      assertEquals( "desc" + i, attr.getDescription() );
    }

    // Verify the referenced MyOtherElement
    //
    MyOtherElement verifyOtherElement = verify.getMyOtherElement();
    assertNotNull( verifyOtherElement );
    assertEquals( myOtherElement.getName(), verifyOtherElement.getName() );
    assertEquals( myOtherElement.getSomeAttribute(), verifyOtherElement.getSomeAttribute() );

    // verify that the String list is loaded...
    List<String> verifyList = verify.getStringList();
    assertEquals( stringList.size(), verifyList.size() );
    for ( int i = 0; i < stringList.size(); i++ ) {
      assertEquals( stringList.get( i ), verifyList.get( i ) );
    }

    List<String> names = factory.getElementNames();
    assertEquals( 1, names.size() );
    assertEquals( NAME, names.get( 0 ) );

    List<MyElement> list = factory.getElements();
    assertEquals( 1, list.size() );
    assertEquals( NAME, list.get( 0 ).getName() );

    factory.deleteElement( NAME );
    assertEquals( 0, factory.getElementNames().size() );
    assertEquals( 0, factory.getElements().size() );
  }

  @Test
  public void testFactoryShared() throws Exception {
    IMetaStore metaStore = new MemoryMetaStore();
    MetaStoreFactory<A> factoryA = new MetaStoreFactory<A>( A.class, metaStore, "hop" );
    MetaStoreFactory<B> factoryB = new MetaStoreFactory<B>( B.class, metaStore, "hop" );
    factoryA.addNameFactory( A.FACTORY_B, factoryB );

    // Construct test-class
    A a = new A( "a" );
    a.getBees().add( new B( "1", true ) );
    a.getBees().add( new B( "2", true ) );
    a.getBees().add( new B( "3", false ) );
    a.getBees().add( new B( "4", true ) );
    a.setB( new B( "b", false ) );

    factoryA.saveElement( a );

    // 1, 2, 4
    //
    assertEquals( 3, factoryB.getElements().size() );

    A _a = factoryA.loadElement( "a" );
    assertNotNull( _a );
    assertEquals( 4, _a.getBees().size() );
    assertEquals( "1", a.getBees().get( 0 ).getName() );
    assertEquals( true, a.getBees().get( 0 ).isShared() );
    assertEquals( "2", a.getBees().get( 1 ).getName() );
    assertEquals( true, a.getBees().get( 1 ).isShared() );
    assertEquals( "3", a.getBees().get( 2 ).getName() );
    assertEquals( false, a.getBees().get( 2 ).isShared() );
    assertEquals( "4", a.getBees().get( 3 ).getName() );
    assertEquals( true, a.getBees().get( 3 ).isShared() );

    assertNotNull( _a.getB() );
    assertEquals( "b", _a.getB().getName() );
    assertEquals( false, _a.getB().isShared() );
  }

  @Test
  public void testFactory() throws Exception {
    IMetaStore metaStore = new MemoryMetaStore();
    MetaStoreFactory<X> factoryX = new MetaStoreFactory<X>( X.class, metaStore, "hop" );
    MetaStoreFactory<Y> factoryY = new MetaStoreFactory<Y>( Y.class, metaStore, "hop" );
    factoryX.addNameFactory( X.FACTORY_Y, factoryY );

    // Construct test-class
    X x = new X( "x" );
    x.getYs().add( new Y( "1", "desc1" ) );
    x.getYs().add( new Y( "2", "desc2" ) );
    x.getYs().add( new Y( "3", "desc3" ) );
    x.getYs().add( new Y( "4", "desc4" ) );
    x.setY( new Y( "y", "descY" ) );

    factoryX.saveElement( x );

    // 1, 2, 3, 4, y
    //
    assertEquals( 5, factoryY.getElements().size() );

    X _x = factoryX.loadElement( "x" );
    assertNotNull( _x );
    assertEquals( 4, _x.getYs().size() );
    assertEquals( "1", x.getYs().get( 0 ).getName() );
    assertEquals( "desc1", x.getYs().get( 0 ).getDescription() );
    assertEquals( "2", x.getYs().get( 1 ).getName() );
    assertEquals( "desc2", x.getYs().get( 1 ).getDescription() );
    assertEquals( "3", x.getYs().get( 2 ).getName() );
    assertEquals( "desc3", x.getYs().get( 2 ).getDescription() );
    assertEquals( "4", x.getYs().get( 3 ).getName() );
    assertEquals( "desc4", x.getYs().get( 3 ).getDescription() );

    assertNotNull( _x.getY() );
    assertEquals( "y", _x.getY().getName() );
    assertEquals( "descY", _x.getY().getDescription() );
  }

  /**
   * Save and load a complete Cube object in the IMetaStore through named references and factories.
   * Some object are saved through a factory with a name reference.  One dimension is embedded in the cube.
   *
   * @throws Exception
   */
  @SuppressWarnings( "unchecked" )
  @Test
  public void testCube() throws Exception {
    IMetaStore metaStore = new MemoryMetaStore();
    MetaStoreFactory<Cube> factoryCube = new MetaStoreFactory<Cube>( Cube.class, metaStore, "hop" );
    MetaStoreFactory<Dimension> factoryDimension = new MetaStoreFactory<Dimension>( Dimension.class, metaStore, "hop" );
    factoryCube.addNameFactory( Cube.DIMENSION_FACTORY_KEY, factoryDimension );
    IMetaStoreObjectFactory objectFactory = mock( IMetaStoreObjectFactory.class );
    factoryCube.setObjectFactory( objectFactory );
    factoryDimension.setObjectFactory( objectFactory );

    final AtomicInteger contextCount = new AtomicInteger( 0 );
    when( objectFactory.getContext( anyObject() ) ).thenAnswer( new Answer<Object>() {
      @Override
      public Object answer( InvocationOnMock invocation ) throws Throwable {
        Map<String, String> context = new HashMap<>();
        context.put( "context-num", String.valueOf( contextCount.getAndIncrement() ) );
        return context;
      }
    } );
    when( objectFactory.instantiateClass( anyString(), anyMap() ) ).thenAnswer( new Answer<Object>() {
      @Override
      public Object answer( InvocationOnMock invocation ) throws Throwable {
        String className = (String) invocation.getArguments()[ 0 ];
        return Class.forName( className ).newInstance();
      }
    } );

    Cube cube = generateCube();
    factoryCube.saveElement( cube );

    // Now load back and verify...
    Cube verify = factoryCube.loadElement( cube.getName() );

    assertEquals( cube.getName(), verify.getName() );
    assertEquals( cube.getDimensions().size(), verify.getDimensions().size() );
    for ( int i = 0; i < cube.getDimensions().size(); i++ ) {
      Dimension dimension = cube.getDimensions().get( i );
      Dimension verifyDimension = verify.getDimensions().get( i );
      assertEquals( dimension.getName(), verifyDimension.getName() );
      assertEquals( dimension.getDimensionType(), verifyDimension.getDimensionType() );
      assertEquals( dimension.getAttributes().size(), verifyDimension.getAttributes().size() );
      for ( int x = 0; x < dimension.getAttributes().size(); x++ ) {
        DimensionAttribute attr = dimension.getAttributes().get( i );
        DimensionAttribute attrVerify = verifyDimension.getAttributes().get( i );
        assertEquals( attr.getName(), attrVerify.getName() );
        assertEquals( attr.getDescription(), attrVerify.getDescription() );
        assertEquals( attr.getSomeOtherStuff(), attrVerify.getSomeOtherStuff() );
      }
    }

    assertEquals( cube.getKpis().size(), verify.getKpis().size() );
    for ( int i = 0; i < cube.getKpis().size(); i++ ) {
      Kpi kpi = cube.getKpis().get( i );
      Kpi verifyKpi = verify.getKpis().get( i );
      assertEquals( kpi.getName(), verifyKpi.getName() );
      assertEquals( kpi.getDescription(), verifyKpi.getDescription() );
      assertEquals( kpi.getOtherDetails(), verifyKpi.getOtherDetails() );
    }

    assertNotNull( verify.getJunkDimension() );
    Dimension junk = cube.getJunkDimension();
    Dimension junkVerify = verify.getJunkDimension();
    assertEquals( junk.getName(), junkVerify.getName() );
    assertEquals( junk.getAttributes().size(), junkVerify.getAttributes().size() );
    for ( int i = 0; i < junk.getAttributes().size(); i++ ) {
      DimensionAttribute attr = junk.getAttributes().get( i );
      DimensionAttribute attrVerify = junkVerify.getAttributes().get( i );
      assertEquals( attr.getName(), attrVerify.getName() );
      assertEquals( attr.getDescription(), attrVerify.getDescription() );
      assertEquals( attr.getSomeOtherStuff(), attrVerify.getSomeOtherStuff() );
    }

    assertNotNull( verify.getNonSharedDimension() );
    Dimension nonShared = cube.getNonSharedDimension();
    Dimension nonSharedVerify = verify.getNonSharedDimension();
    assertEquals( nonShared.getName(), nonSharedVerify.getName() );
    assertEquals( nonShared.getAttributes().size(), nonSharedVerify.getAttributes().size() );
    for ( int i = 0; i < junk.getAttributes().size(); i++ ) {
      DimensionAttribute attr = nonShared.getAttributes().get( i );
      DimensionAttribute attrVerify = nonSharedVerify.getAttributes().get( i );
      assertEquals( attr.getName(), attrVerify.getName() );
      assertEquals( attr.getDescription(), attrVerify.getDescription() );
      assertEquals( attr.getSomeOtherStuff(), attrVerify.getSomeOtherStuff() );
    }

    // Make sure that nonShared and product are not shared.
    // We can load them with the dimension factory and they should not come back.
    //
    assertNull( factoryDimension.loadElement( "analyticalDim" ) );
    assertNull( factoryDimension.loadElement( "product" ) );

    assertNotNull( verify.getMainKpi() );
    assertEquals( cube.getMainKpi().getName(), verify.getMainKpi().getName() );
    assertEquals( cube.getMainKpi().getDescription(), verify.getMainKpi().getDescription() );
    assertEquals( cube.getMainKpi().getOtherDetails(), verify.getMainKpi().getOtherDetails() );

    for ( int i = 0; i < contextCount.get(); i++ ) {
      Map<String, String> context = new HashMap<>();
      context.put( "context-num", String.valueOf( i ) );
      verify( objectFactory ).instantiateClass( anyString(), eq( context ) );
    }

  }

  private Cube generateCube() {
    Cube cube = new Cube();
    cube.setName( "Fact" );

    Dimension customer = new Dimension();
    customer.setName( "customer" );
    customer.setAttributes( generateAttributes() );
    customer.setDimensionType( DimensionType.SCD );
    cube.getDimensions().add( customer );

    Dimension product = new Dimension();
    product.setName( "product" );
    product.setAttributes( generateAttributes() );
    product.setDimensionType( null );
    product.setShared( false );
    cube.getDimensions().add( product );

    Dimension date = new Dimension();
    date.setName( "date" );
    date.setAttributes( generateAttributes() );
    date.setDimensionType( DimensionType.DATE );
    cube.getDimensions().add( date );

    Dimension junk = new Dimension();
    junk.setName( "junk" );
    junk.setAttributes( generateAttributes() );
    junk.setDimensionType( DimensionType.JUNK );
    cube.setJunkDimension( junk );

    Dimension nonShared = new Dimension();
    nonShared.setName( "analyticalDim" );
    nonShared.setAttributes( generateAttributes() );
    nonShared.setDimensionType( DimensionType.JUNK );
    nonShared.setShared( false );
    cube.setNonSharedDimension( nonShared );

    cube.setKpis( generateKpis() );

    Kpi mainKpi = new Kpi();
    mainKpi.setName( "mainKpi-name" );
    mainKpi.setDescription( "mainKpi-description" );
    mainKpi.setOtherDetails( "mainKpi-otherDetails" );
    cube.setMainKpi( mainKpi );

    return cube;
  }

  public void testSanitizeName() throws Exception {
    IMetaStore metaStore = new MemoryMetaStore();
    MetaStoreFactory<MyOtherElement> factory = new MetaStoreFactory<MyOtherElement>( MyOtherElement.class, metaStore, "custom" );
    MyOtherElement element = new MyOtherElement( null, ATTR );

    try {
      factory.saveElement( element );
      fail( "Saved illegal element (name == null)" );
    } catch ( MetaStoreException e ) {
      assertNotNull( e );
    }

    try {
      element.setName( "" );
      factory.saveElement( element );
      fail( "Saved illegal element (name.isEmpty())" );
    } catch ( MetaStoreException e ) {
      assertNotNull( e );
    }

    try {
      element.setName( " " );
      factory.saveElement( element );
      fail( "Saved illegal element (name.isEmpty())" );
    } catch ( MetaStoreException e ) {
      assertNotNull( e );
    }

    element.setName( NAME );
    factory.saveElement( element );
    assertEquals( Arrays.asList( NAME ), factory.getElementNames() );
  }

  private List<Kpi> generateKpis() {
    List<Kpi> list = new ArrayList<Kpi>();
    for ( int i = 0; i < 5; i++ ) {
      Kpi kpi = new Kpi();
      kpi.setName( "kpi-" + ( i + 1 ) );
      kpi.setDescription( "desc-" + ( i + 1 ) );
      kpi.setOtherDetails( "othd-" + ( i + 1 ) );
    }
    return list;
  }

  private List<DimensionAttribute> generateAttributes() {
    List<DimensionAttribute> list = new ArrayList<DimensionAttribute>();
    for ( int i = 0; i < 10; i++ ) {
      DimensionAttribute attribute = new DimensionAttribute();
      attribute.setName( "attr-" + ( i + 1 ) );
      attribute.setDescription( "desc-" + ( i + 1 ) );
      attribute.setSomeOtherStuff( "other" + ( i + 1 ) );
      list.add( attribute );
    }
    return list;
  }

}
