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

package org.apache.hop.core.auth;

import org.apache.hop.core.auth.core.IAuthenticationProvider;
@AuthenticationProviderPlugin (
  id = "UsernameAndPassword", 
  name = "UsernameAndPassword",
  description = "Username And Password"
)
public class UsernamePasswordAuthenticationProvider implements IAuthenticationProvider {
  public static class UsernamePasswordAuthenticationProviderType implements IAuthenticationProviderType {

    @Override
    public String getDisplayName() {
      return UsernamePasswordAuthenticationProvider.class.getName();
    }

    @Override
    public Class<? extends IAuthenticationProvider> getProviderClass() {
      return UsernamePasswordAuthenticationProvider.class;
    }
  }

  private String id;
  private String username;
  private String password;

  public UsernamePasswordAuthenticationProvider() {

  }

  public UsernamePasswordAuthenticationProvider( String id, String username, String password ) {
    this.id = id;
    this.username = username;
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername( String username ) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword( String password ) {
    this.password = password;
  }

  @Override
  public String getDisplayName() {
    return username;
  }

  @Override
  public String getId() {
    return id;
  }

  public void setId( String id ) {
    this.id = id;
  }
}
