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
package org.apache.hop.databases.redshift;

/** Which flavour of Redshift is on the other end, which decides how the host name is arrived at. */
public enum RedshiftDeploymentType {
  /**
   * A provisioned cluster. The server host name is entered as is, the way this plugin has always
   * worked. Kept as the default so existing connections keep connecting exactly as before.
   */
  PROVISIONED,

  /**
   * A serverless workgroup. Its endpoint is entirely predictable from the workgroup name, the AWS
   * account number and the region, so Hop builds it rather than asking for a host name that is easy
   * to get subtly wrong.
   */
  SERVERLESS
}
