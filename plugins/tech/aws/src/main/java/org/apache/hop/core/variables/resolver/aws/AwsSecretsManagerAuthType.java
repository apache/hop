/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.variables.resolver.aws;

/** Authentication method used by the AWS Secrets Manager variable resolver. */
public enum AwsSecretsManagerAuthType {
  /**
   * Let the AWS SDK find credentials by itself, through the default provider chain: system
   * properties, environment variables, a web identity token, the credentials file, container
   * credentials and finally the EC2 instance profile.
   */
  AUTOMATIC,
  /** An explicit access key and secret key, optionally with a session token. */
  ACCESS_KEYS,
  /** An AWS credentials file, optionally with a profile name. */
  CREDENTIALS_FILE,
}
