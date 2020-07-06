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

package org.apache.hop.pipeline.transforms.replacestring;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Created by bmorrise on 3/21/16.
 */
public class ReplaceStringMetaInjectionTest extends BaseMetadataInjectionTest<ReplaceStringMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new ReplaceStringMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FIELD_IN_STREAM", new IStringGetter() {
      @Override public String get() {
        return meta.getFieldInStream()[ 0 ];
      }
    } );
    check( "FIELD_OUT_STREAM", new IStringGetter() {
      @Override public String get() {
        return meta.getFieldOutStream()[ 0 ];
      }
    } );
    check( "USE_REGEX", new IIntGetter() {
      @Override public int get() {
        return meta.getUseRegEx()[ 0 ];
      }
    } );
    check( "REPLACE_STRING", new IStringGetter() {
      @Override public String get() {
        return meta.getReplaceString()[ 0 ];
      }
    } );
    check( "REPLACE_BY", new IStringGetter() {
      @Override public String get() {
        return meta.getReplaceByString()[ 0 ];
      }
    } );
    check( "EMPTY_STRING", new IBooleanGetter() {
      @Override public boolean get() {
        return meta.isSetEmptyString()[ 0 ];
      }
    } );
    check( "REPLACE_WITH_FIELD", new IStringGetter() {
      @Override public String get() {
        return meta.getFieldReplaceByString()[ 0 ];
      }
    } );
    check( "REPLACE_WHOLE_WORD", new IIntGetter() {
      @Override public int get() {
        return meta.getWholeWord()[ 0 ];
      }
    } );
    check( "CASE_SENSITIVE", new IIntGetter() {
      @Override public int get() {
        return meta.getCaseSensitive()[ 0 ];
      }
    } );
    check( "IS_UNICODE", new IIntGetter() {
      @Override public int get() {
        return meta.isUnicode()[ 0 ];
      }
    } );
  }
}
