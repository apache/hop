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
 *
 */

package org.apache.hop.vfs.azure.config;

public class AzureConfig {

  public static final String HOP_CONFIG_AZURE_CONFIG_KEY = "azure";

  private String account;
  private String key;
  private String emulatorUrl;

  public AzureConfig() {}

  public AzureConfig(AzureConfig config) {
    this();
    this.account = config.account;
    this.key = config.key;
    this.emulatorUrl = config.emulatorUrl;
  }

  /**
   * Gets account
   *
   * @return value of account
   */
  public String getAccount() {
    return account;
  }

  /**
   * @param account The account to set
   */
  public void setAccount(String account) {
    this.account = account;
  }

  /**
   * Gets key
   *
   * @return value of key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key The key to set
   */
  public void setKey(String key) {
    this.key = key;
  }

  /**
   * Gets emulatorUrl
   *
   * @return the url of the Azure Emulator
   */
  public String getEmulatorUrl() {
    return emulatorUrl;
  }

  /**
   * @param emulatorUrl The emulatorUrl to set
   */
  public void setEmulatorUrl(String emulatorUrl) {
    this.emulatorUrl = emulatorUrl;
  }
}
