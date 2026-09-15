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

/**
 * How the Redshift JDBC driver is asked to authenticate. Everything other than {@link #DATABASE}
 * makes the driver fetch temporary database credentials from AWS, which it only does when the URL
 * carries the {@code iam} marker: {@code jdbc:redshift:iam://...}.
 */
public enum RedshiftAuthenticationType {
  /**
   * A Redshift database user and password, the way this plugin has always worked. Kept as the
   * default so existing connections keep connecting exactly as before.
   */
  DATABASE,

  /**
   * An AWS access key and secret access key, optionally with a session token for temporary
   * credentials. Passed to the driver as {@code AccessKeyID} / {@code SecretAccessKey} / {@code
   * SessionToken}.
   */
  IAM_CREDENTIALS,

  /**
   * A named profile from the shared AWS credentials file, passed as {@code Profile}. Nothing secret
   * ends up in the Hop metadata, which makes this the friendlier option for a shared project.
   */
  IAM_PROFILE,

  /**
   * Whatever the AWS default credentials chain finds: environment variables, the shared credentials
   * file, an SSO session, an EC2 instance profile, an ECS task role, ... Nothing is passed to the
   * driver, which is exactly what makes it fall back to that chain. This is the one to use when Hop
   * runs inside AWS.
   */
  IAM_DEFAULT_CHAIN;

  /**
   * @return true if this type makes the driver ask AWS for temporary database credentials
   */
  public boolean isIam() {
    return this != DATABASE;
  }
}
