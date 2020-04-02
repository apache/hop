/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2016-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class DimensionLookupMetaInjectionTest extends BaseMetadataInjectionTest<DimensionLookupMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    super.setup( new DimensionLookupMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "TARGET_SCHEMA", new IStringGetter() {
      @Override
      public String get() {
        return meta.getSchemaName();
      }
    } );
    check( "TARGET_TABLE", new IStringGetter() {
      @Override
      public String get() {
        return meta.getTableName();
      }
    } );
    check( "UPDATE_DIMENSION", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.isUpdate();
      }
    } );
    check( "KEY_STREAM_FIELDNAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyStream()[ 0 ];
      }
    } );
    check( "KEY_DATABASE_FIELDNAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyLookup()[ 0 ];
      }
    } );
    check( "STREAM_DATE_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getDateField();
      }
    } );
    check( "DATE_RANGE_START_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getDateFrom();
      }
    } );
    check( "DATE_RANGE_END_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getDateTo();
      }
    } );
    check( "STREAM_FIELDNAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getFieldStream()[ 0 ];
      }
    } );
    check( "DATABASE_FIELDNAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getFieldLookup()[ 0 ];
      }
    } );
    check( "TECHNICAL_KEY_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyField();
      }
    } );
    check( "TECHNICAL_KEY_NEW_NAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyRename();
      }
    } );
    check( "VERSION_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getVersionField();
      }
    } );
    check( "TECHNICAL_KEY_SEQUENCE", new IStringGetter() {
      @Override
      public String get() {
        return meta.getSequenceName();
      }
    } );
    check( "COMMIT_SIZE", new IIntGetter() {
      @Override
      public int get() {
        return meta.getCommitSize();
      }
    } );
    check( "MIN_YEAR", new IIntGetter() {
      @Override
      public int get() {
        return meta.getMinYear();
      }
    } );
    check( "MAX_YEAR", new IIntGetter() {
      @Override
      public int get() {
        return meta.getMaxYear();
      }
    } );
    check( "TECHNICAL_KEY_CREATION", new IStringGetter() {
      @Override
      public String get() {
        return meta.getTechKeyCreation();
      }
    } );
    check( "CACHE_SIZE", new IIntGetter() {
      @Override
      public int get() {
        return meta.getCacheSize();
      }
    } );
    check( "USE_ALTERNATIVE_START_DATE", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.isUsingStartDateAlternative();
      }
    } );
    check( "ALTERNATIVE_START_COLUMN", new IStringGetter() {
      @Override
      public String get() {
        return meta.getStartDateFieldName();
      }
    } );
    check( "PRELOAD_CACHE", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.isPreloadingCache();
      }
    } );
    check( "CONNECTION_NAME", new IStringGetter() {
      public String get() {
        return "My Connection";
      }
    }, "My Connection" );

    IValueMeta mftt = new ValueMetaString( "f" );
    injector.setProperty( meta, "ALTERNATIVE_START_OPTION", setValue( mftt, DimensionLookupMeta
      .getStartDateAlternativeCode( 0 ) ), "f" );
    Assert.assertEquals( 0, meta.getStartDateAlternative() );

    String[] valueMetaNames = ValueMetaFactory.getValueMetaNames();
    checkStringToInt( "TYPE_OF_RETURN_FIELD", new IIntGetter() {
      @Override
      public int get() {
        return meta.getReturnType()[ 0 ];
      }
    }, valueMetaNames, getTypeCodes( valueMetaNames ) );

    skipPropertyTest( "ALTERNATIVE_START_OPTION" );

    skipPropertyTest( "UPDATE_TYPE" );
  }

}
