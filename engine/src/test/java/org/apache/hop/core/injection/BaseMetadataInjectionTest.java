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

package org.apache.hop.core.injection;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.junit.After;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base class for test metadata injection.
 */
@Ignore
public abstract class BaseMetadataInjectionTest<Meta extends ITransformMeta<?, ?>> {
  protected BeanInjectionInfo<Meta> info;
  protected BeanInjector<Meta> injector;
  protected Meta meta;
  protected Set<String> nonTestedProperties;
  protected IHopMetadataProvider metadataProvider;

  protected void setup( Meta meta ) throws Exception {
    HopClientEnvironment.init();

    this.meta = meta;
    this.metadataProvider = new MemoryMetadataProvider();
    info = new BeanInjectionInfo( meta.getClass() );
    injector = new BeanInjector( info );
    nonTestedProperties = new HashSet<>( info.getProperties().keySet() );
  }

  @After
  public void after() {
    assertTrue( "Some properties where not tested: " + nonTestedProperties, nonTestedProperties.isEmpty() );
  }

  protected List<RowMetaAndData> setValue( IValueMeta valueMeta, Object... values ) {
    RowMeta rowsMeta = new RowMeta();
    rowsMeta.addValueMeta( valueMeta );
    List<RowMetaAndData> rows = new ArrayList<>();
    if ( values != null ) {
      for ( Object v : values ) {
        rows.add( new RowMetaAndData( rowsMeta, v ) );
      }
    }
    return rows;
  }

  protected void skipPropertyTest( String propertyName ) {
    nonTestedProperties.remove( propertyName );
  }

  /**
   * Check boolean property.
   */
  protected void check( String propertyName, IBooleanGetter getter ) throws HopException {
    IValueMeta valueMetaString = new ValueMetaString( "f" );

    injector.setProperty( meta, propertyName, setValue( valueMetaString, "Y" ), "f" );
    assertEquals( true, getter.get() );

    injector.setProperty( meta, propertyName, setValue( valueMetaString, "N" ), "f" );
    assertEquals( false, getter.get() );

    IValueMeta valueMetaBoolean = new ValueMetaBoolean( "f" );

    injector.setProperty( meta, propertyName, setValue( valueMetaBoolean, true ), "f" );
    assertEquals( true, getter.get() );

    injector.setProperty( meta, propertyName, setValue( valueMetaBoolean, false ), "f" );
    assertEquals( false, getter.get() );

    skipPropertyTest( propertyName );
  }

  /**
   * Check string property.
   */
  protected void check( String propertyName, IStringGetter getter, String... values ) throws HopException {
    IValueMeta valueMeta = new ValueMetaString( "f" );

    if ( values.length == 0 ) {
      values = new String[] { "v", "v2", null };
    }

    String correctValue = null;
    for ( String v : values ) {
      injector.setProperty( meta, propertyName, setValue( valueMeta, v ), "f" );
      if ( v != null ) {
        // only not-null values injected
        correctValue = v;
      }
      assertEquals( correctValue, getter.get() );
    }

    skipPropertyTest( propertyName );
  }

  /**
   * Check enum property.
   */
  protected void check( String propertyName, IEnumGetter getter, Class<?> enumType ) throws HopException {
    IValueMeta valueMeta = new ValueMetaString( "f" );

    Object[] values = enumType.getEnumConstants();

    for ( Object v : values ) {
      injector.setProperty( meta, propertyName, setValue( valueMeta, v ), "f" );
      assertEquals( v, getter.get() );
    }

    try {
      injector.setProperty( meta, propertyName, setValue( valueMeta, "###" ), "f" );
      fail( "Should be passed to enum" );
    } catch ( HopException ex ) {
    }

    skipPropertyTest( propertyName );
  }

  /**
   * Check int property.
   */
  protected void check( String propertyName, IIntGetter getter ) throws HopException {
    IValueMeta valueMetaString = new ValueMetaString( "f" );

    injector.setProperty( meta, propertyName, setValue( valueMetaString, "1" ), "f" );
    assertEquals( 1, getter.get() );

    injector.setProperty( meta, propertyName, setValue( valueMetaString, "45" ), "f" );
    assertEquals( 45, getter.get() );

    IValueMeta valueMetaInteger = new ValueMetaInteger( "f" );

    injector.setProperty( meta, propertyName, setValue( valueMetaInteger, 1234L ), "f" );
    assertEquals( 1234, getter.get() );

    injector.setProperty( meta, propertyName, setValue( valueMetaInteger, (long) Integer.MAX_VALUE ), "f" );
    assertEquals( Integer.MAX_VALUE, getter.get() );

    skipPropertyTest( propertyName );
  }

  /**
   * Check string-to-int property.
   */
  protected void checkStringToInt( String propertyName, IIntGetter getter, String[] codes, int[] ids )
    throws HopException {
    if ( codes.length != ids.length ) {
      throw new RuntimeException( "Wrong codes/ids sizes" );
    }
    IValueMeta valueMetaString = new ValueMetaString( "f" );

    for ( int i = 0; i < codes.length; i++ ) {
      injector.setProperty( meta, propertyName, setValue( valueMetaString, codes[ i ] ), "f" );
      assertEquals( ids[ i ], getter.get() );
    }

    skipPropertyTest( propertyName );
  }

  /**
   * Check long property.
   */
  protected void check( String propertyName, ILongGetter getter ) throws HopException {
    IValueMeta valueMetaString = new ValueMetaString( "f" );

    injector.setProperty( meta, propertyName, setValue( valueMetaString, "1" ), "f" );
    assertEquals( 1, getter.get() );

    injector.setProperty( meta, propertyName, setValue( valueMetaString, "45" ), "f" );
    assertEquals( 45, getter.get() );

    IValueMeta valueMetaInteger = new ValueMetaInteger( "f" );

    injector.setProperty( meta, propertyName, setValue( valueMetaInteger, 1234L ), "f" );
    assertEquals( 1234, getter.get() );

    injector.setProperty( meta, propertyName, setValue( valueMetaInteger, Long.MAX_VALUE ), "f" );
    assertEquals( Long.MAX_VALUE, getter.get() );

    skipPropertyTest( propertyName );
  }

  public static int[] getTypeCodes( String[] typeNames ) {
    int[] typeCodes = new int[ typeNames.length ];
    for ( int i = 0; i < typeNames.length; i++ ) {
      typeCodes[ i ] = ValueMetaBase.getType( typeNames[ i ] );
    }
    return typeCodes;
  }

  public interface IBooleanGetter {
    boolean get();
  }

  public interface IStringGetter {
    String get();
  }

  public interface IEnumGetter {
    Enum<?> get();
  }

  public interface IIntGetter {
    int get();
  }

  public interface ILongGetter {
    long get();
  }
}
