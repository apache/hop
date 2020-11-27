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
package org.apache.hop.pipeline.transforms.metainject;

import java.util.*;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.MapLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class MetaInjectMetaLoadSaveTest {
  @ClassRule
  public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;
  Class<MetaInjectMeta> testMetaClass = MetaInjectMeta.class;

  @Before
  public void setUpLoadSave() throws Exception {
    List<String> attributes =
            Arrays.asList( "fileName", "sourceTransformName", "targetFile",
                    "noExecution", "streamSourceTransformName", "streamTargetTransformName",
                    "sourceOutputFields" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "sourceOutputFields",
      new ListLoadSaveValidator<>( new MetaInjectOutputFieldLoadSaveValidator(), 5 ) );
    //
    // Note - these seem to be runtime-built and not persisted.
        attrValidatorMap.put( "metaInjectMapping",
          new ListLoadSaveValidator<>( new MetaInjectMappingLoadSaveValidator(), 5 ) );
        attrValidatorMap.put( "targetSourceMapping",
          new MapLoadSaveValidator<>(
            new TargetTransformAttributeLoadSaveValidator(),
            new SourceTransformFieldLoadSaveValidator(),
            5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester = new LoadSaveTester( testMetaClass, attributes, getterMap, setterMap,
                attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class MetaInjectOutputFieldLoadSaveValidator implements IFieldLoadSaveValidator<MetaInjectOutputField> {
    final Random rand = new Random();
    @Override
    public MetaInjectOutputField getTestObject() {
      MetaInjectOutputField rtn = new MetaInjectOutputField();
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setLength( rand.nextInt( 100 ) );
      rtn.setPrecision( rand.nextInt( 9 ) );
      rtn.setType( rand.nextInt( 7 ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( MetaInjectOutputField testObject, Object actual ) {
      if ( !( actual instanceof MetaInjectOutputField ) ) {
        return false;
      }
      MetaInjectOutputField another = (MetaInjectOutputField) actual;
      return new EqualsBuilder()
        .append( testObject.getLength(), another.getLength() )
        .append( testObject.getPrecision(), another.getPrecision() )
        .append( testObject.getName(), another.getName() )
        .append( testObject.getType(), another.getType() )
        .isEquals();
    }
  }

  //MetaInjectMappingLoadSaveValidator
  public class MetaInjectMappingLoadSaveValidator implements IFieldLoadSaveValidator<MetaInjectMapping> {
    final Random rand = new Random();
    @Override
    public MetaInjectMapping getTestObject() {
      MetaInjectMapping rtn = new MetaInjectMapping();
      rtn.setSourceField( UUID.randomUUID().toString() );
      rtn.setSourceTransform( UUID.randomUUID().toString() );
      rtn.setTargetField( UUID.randomUUID().toString() );
      rtn.setTargetTransform( UUID.randomUUID().toString() );
      return rtn;
    }

    @Override
    public boolean validateTestObject( MetaInjectMapping testObject, Object actual ) {
      if ( !( actual instanceof MetaInjectMapping ) ) {
        return false;
      }
      MetaInjectMapping another = (MetaInjectMapping) actual;
      return new EqualsBuilder()
        .append( testObject.getSourceField(), another.getSourceField() )
        .append( testObject.getSourceTransform(), another.getSourceTransform() )
        .append( testObject.getTargetField(), another.getTargetField() )
        .append( testObject.getTargetTransform(), another.getTargetTransform() )
        .isEquals();
    }
  }
  // TargetTransformAttributeLoadSaveValidator
  public class TargetTransformAttributeLoadSaveValidator implements IFieldLoadSaveValidator<TargetTransformAttribute> {
    final Random rand = new Random();
    @Override
    public TargetTransformAttribute getTestObject() {
      return  new TargetTransformAttribute( UUID.randomUUID().toString(), UUID.randomUUID().toString(), rand.nextBoolean() );
    }

    @Override
    public boolean validateTestObject(TargetTransformAttribute testObject, Object actual ) {
      if ( !( actual instanceof TargetTransformAttribute) ) {
        return false;
      }
      TargetTransformAttribute another = (TargetTransformAttribute) actual;
      return new EqualsBuilder()
          .append( testObject.getTransformName(), another.getTransformName() )
          .append( testObject.getAttributeKey(), another.getAttributeKey() )
          .append( testObject.isDetail(), another.isDetail() )
      .isEquals();
    }
  }

  // SourceTransformFieldLoadSaveValidator
  public class SourceTransformFieldLoadSaveValidator implements IFieldLoadSaveValidator<SourceTransformField> {
    final Random rand = new Random();
    @Override
    public SourceTransformField getTestObject() {
      return  new SourceTransformField( UUID.randomUUID().toString(), UUID.randomUUID().toString() );
    }

    @Override
    public boolean validateTestObject(SourceTransformField testObject, Object actual ) {
      if ( !( actual instanceof SourceTransformField) ) {
        return false;
      }
      SourceTransformField another = (SourceTransformField) actual;
      return new EqualsBuilder()
          .append( testObject.getTransformName(), another.getTransformName() )
          .append( testObject.getField(), another.getField() )
      .isEquals();
    }
  }
}
