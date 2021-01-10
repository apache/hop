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

package org.apache.hop.pipeline.transforms.rest;

import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import javax.ws.rs.core.MediaType;

/**
 * @author Samatar
 * @since 16-jan-2011
 */
public class RestData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;
  public IRowMeta inputRowMeta;

  /**
   * URL
   **/
  public int indexOfUrlField;
  public String realUrl;
  /**
   * Method
   **/
  public String method;
  /**
   * Index of method
   **/
  public int indexOfMethod;

  public int nrheader;
  /**
   * Headers
   **/
  public int[] indexOfHeaderFields;
  public String[] headerNames;

  /**
   * query parameters
   **/
  public int nrParams;
  public int[] indexOfParamFields;
  public String[] paramNames;

  /**
   * matrix parameters
   **/
  public int nrMatrixParams;
  public int[] indexOfMatrixParamFields;
  public String[] matrixParamNames;

  /**
   * proxy
   **/
  public String realProxyHost;
  public int realProxyPort;
  public String realHttpLogin;
  public String realHttpPassword;

  /**
   * Result fieldnames
   **/
  public String resultFieldName;
  public String resultCodeFieldName;
  public String resultResponseFieldName;
  public String resultHeaderFieldName;

  /**
   * Flag set headers
   **/
  public boolean useHeaders;

  /**
   * Flag set Query Parameters
   **/
  public boolean useParams;

  /**
   * Flag set Matrix Parameters
   **/
  public boolean useMatrixParams;

  /**
   * Flag set body
   **/
  public boolean useBody;

  /**
   * Index of body field
   **/
  public int indexOfBodyField;

  /**
   * trust store
   **/
  public String trustStoreFile;
  public String trustStorePassword;

  public DefaultApacheHttpClient4Config config;

  public HTTPBasicAuthFilter basicAuthentication;

  public MediaType mediaType;

  public RestData() {
    super();
    this.indexOfUrlField = -1;

    this.realProxyHost = null;
    this.realProxyPort = 8080;
    this.realHttpLogin = null;
    this.realHttpPassword = null;
    this.resultFieldName = null;
    this.resultCodeFieldName = null;
    this.resultResponseFieldName = null;
    this.resultHeaderFieldName = null;
    this.nrheader = 0;
    this.nrParams = 0;
    this.nrMatrixParams = 0;
    this.method = null;
    this.indexOfBodyField = -1;
    this.indexOfMethod = -1;
    this.config = null;
    this.trustStoreFile = null;
    this.trustStorePassword = null;
    this.basicAuthentication = null;
  }

}
