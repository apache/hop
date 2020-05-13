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

package org.apache.hop.pipeline.transforms.sort;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class SortRowsMetaInjectionTest extends BaseMetadataInjectionTest<SortRowsMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new SortRowsMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SORT_DIRECTORY", new IStringGetter() {
      @Override
      public String get() {
        return meta.getDirectory();
      }
    } );
    check( "SORT_FILE_PREFIX", new IStringGetter() {
      @Override
      public String get() {
        return meta.getPrefix();
      }
    } );
    check( "SORT_SIZE_ROWS", new IStringGetter() {
      @Override
      public String get() {
        return meta.getSortSize();
      }
    } );
    check( "FREE_MEMORY_TRESHOLD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getFreeMemoryLimit();
      }
    } );
    check( "ONLY_PASS_UNIQUE_ROWS", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.isOnlyPassingUniqueRows();
      }
    } );
    check( "COMPRESS_TEMP_FILES", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.getCompressFiles();
      }
    } );
    check( "NAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getFieldName()[ 0 ];
      }
    } );
    check( "SORT_ASCENDING", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.getAscending()[ 0 ];
      }
    } );
    check( "IGNORE_CASE", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.getCaseSensitive()[ 0 ];
      }
    } );
    check( "PRESORTED", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.getPreSortedField()[ 0 ];
      }
    } );
    check( "COLLATOR_STRENGTH", new IIntGetter() {
      @Override
      public int get() {
        return meta.getCollatorStrength()[ 0 ];
      }
    } );
    check( "COLLATOR_ENABLED", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.getCollatorEnabled()[ 0 ];
      }
    } );
  }
}
