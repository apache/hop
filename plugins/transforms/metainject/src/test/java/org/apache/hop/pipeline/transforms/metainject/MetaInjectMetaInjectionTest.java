/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.metainject;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class MetaInjectMetaInjectionTest extends BaseMetadataInjectionTest<MetaInjectMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static final String TEST_ID = "TEST_ID";

  @Before
  public void setup() throws Exception{
      setup( new MetaInjectMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "PIPELINE_NAME", new IStringGetter() {
      public String get() {
        return meta.getPipelineName();
      }
    } );
    check( "FILE_NAME", new IStringGetter() {
      public String get() {
        return meta.getFileName();
      }
    } );
    check( "DIRECTORY_PATH", new IStringGetter() {
      public String get() {
        return meta.getDirectoryPath();
      }
    } );
    check( "SOURCE_TRANSFORM_NAME", new IStringGetter() {
      public String get() {
        return meta.getSourceTransformName();
      }
    } );
    check( "TARGET_FILE", new IStringGetter() {
      public String get() {
        return meta.getTargetFile();
      }
    } );
    check( "NO_EXECUTION", new IBooleanGetter() {
      public boolean get() {
        return meta.isNoExecution();
      }
    } );
    check( "STREAMING_SOURCE_TRANSFORM", new IStringGetter() {
      public String get() {
        return meta.getStreamSourceTransformName();
      }
    } );
    check( "STREAMING_TARGET_TRANSFORM", new IStringGetter() {
      public String get() {
        return meta.getStreamTargetTransformName();
      }
    } );
    check( "SOURCE_OUTPUT_NAME", new IStringGetter() {
      public String get() {
        return meta.getSourceOutputFields().get( 0 ).getName();
      }
    } );
    String[] typeNames = ValueMetaBase.getAllTypes();

    checkStringToInt( "SOURCE_OUTPUT_TYPE", new IIntGetter() {
      public int get() {
        return meta.getSourceOutputFields().get( 0 ).getType();
      }
    }, typeNames, getTypeCodes( typeNames ) );
    check( "SOURCE_OUTPUT_LENGTH", new IIntGetter() {
      public int get() {
        return meta.getSourceOutputFields().get( 0 ).getLength();
      }
    } );
    check( "SOURCE_OUTPUT_PRECISION", new IIntGetter() {
      public int get() {
        return meta.getSourceOutputFields().get( 0 ).getPrecision();
      }
    } );
    check( "MAPPING_SOURCE_TRANSFORM", new IStringGetter() {
      public String get() {
        return meta.getMetaInjectMapping().get( 0 ).getSourceTransform();
      }
    } );
    check( "MAPPING_SOURCE_FIELD", new IStringGetter() {
      public String get() {
        return meta.getMetaInjectMapping().get( 0 ).getSourceField();
      }
    } );
    check( "MAPPING_TARGET_TRANSFORM", new IStringGetter() {
      public String get() {
        return meta.getMetaInjectMapping().get( 0 ).getTargetTransform();
      }
    } );
    check( "MAPPING_TARGET_FIELD", new IStringGetter() {
      public String get() {
        return meta.getMetaInjectMapping().get( 0 ).getTargetField();
      }
    } );
    check( "PIPELINE_SPECIFICATION_METHOD", new IEnumGetter() {
      @Override
      public Enum<?> get() {
        return meta.getSpecificationMethod();
      }

    }, ObjectLocationSpecificationMethod.class );

    IValueMeta mftt = new ValueMetaString( "f" );
//    injector.setProperty( meta, "TRANS_OBJECT_ID", setValue( mftt, TEST_ID ), "f" );
//    assertEquals( TEST_ID, meta.getTransObjectId().getId() );
//    skipPropertyTest( "TRANS_OBJECT_ID" );
  }

}
