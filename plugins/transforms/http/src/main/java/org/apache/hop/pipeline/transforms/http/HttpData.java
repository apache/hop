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

package org.apache.hop.pipeline.transforms.http;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.http.NameValuePair;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class HttpData extends BaseTransformData implements ITransformData {
  public int[] argnrs;
  public IRowMeta outputRowMeta;
  public IRowMeta inputRowMeta;
  public int indexOfUrlField;
  public String realUrl;
  public String realProxyHost;
  public int realProxyPort;
  public String realHttpLogin;
  public String realHttpPassword;
  public int[] header_parameters_nrs;
  public boolean useHeaderParameters;
  public NameValuePair[] headerParameters;

  public int realSocketTimeout;
  public int realConnectionTimeout;
  public int realcloseIdleConnectionsTime;

  /**
   * Default constructor.
   */
  public HttpData() {
    super();
    indexOfUrlField = -1;
    realProxyHost = null;
    realProxyPort = 8080;
    realHttpLogin = null;
    realHttpPassword = null;
  }
}
