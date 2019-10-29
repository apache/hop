/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.job.JobMeta;
import org.apache.hop.repository.RepositoryObjectType;
import org.apache.hop.trans.TransMeta;

public class EngineMetaUtilsTest {

  @Test
  public void isJobOrTransformation_withJob() {
    JobMeta jobInstance = new JobMeta();
    assertTrue( EngineMetaUtils.isJobOrTransformation( jobInstance ) );
  }

  @Test
  public void isJobOrTransformation_withTransformation() {
    TransMeta transfromataionInstance = new TransMeta();
    assertTrue( EngineMetaUtils.isJobOrTransformation( transfromataionInstance ) );
  }

  @Test
  public void isJobOrTransformationReturnsFalse_withDatabase() {
    EngineMetaInterface testMetaInstance = mock( EngineMetaInterface.class );
    when( testMetaInstance.getRepositoryElementType() ).thenReturn( RepositoryObjectType.DATABASE );
    assertFalse( EngineMetaUtils.isJobOrTransformation( testMetaInstance ) );
  }

}
