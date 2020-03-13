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
package org.apache.hop.job.entries.snmptrap;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JobEntrySNMPTrapTest extends JobEntryLoadSaveTestSupport<JobEntrySNMPTrap> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );
  }

  @Override
  protected Class<JobEntrySNMPTrap> getJobEntryClass() {
    return JobEntrySNMPTrap.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "serverName",
      "port",
      "timeout",
      "nrretry",
      "comString",
      "message",
      "oid",
      "targettype",
      "user",
      "passphrase",
      "engineid" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "serverName", "getServerName",
      "port", "getPort",
      "timeout", "getTimeout",
      "nrretry", "getRetry",
      "comString", "getComString",
      "message", "getMessage",
      "oid", "getOID",
      "targettype", "getTargetType",
      "user", "getUser",
      "passphrase", "getPassPhrase",
      "engineid", "getEngineID" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "serverName", "setServerName",
      "port", "setPort",
      "timeout", "setTimeout",
      "nrretry", "setRetry",
      "comString", "setComString",
      "message", "setMessage",
      "oid", "setOID",
      "targettype", "setTargetType",
      "user", "setUser",
      "passphrase", "setPassPhrase",
      "engineid", "setEngineID" );
  }

}
