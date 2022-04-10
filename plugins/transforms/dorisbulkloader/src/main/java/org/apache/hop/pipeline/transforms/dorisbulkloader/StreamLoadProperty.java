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
 
package org.apache.hop.pipeline.transforms.dorisbulkloader;

import java.util.Map;

public class StreamLoadProperty {
    /** doris fe host */
    private String feHost;

    /** doris http port */
    private String feHttpPort;

    /** doris database name */
    private String databaseName;

    /** doris table name */
    private String tableName;

    /** doris login user */
    private String loginUser;

    /** doris login password */
    private String loginPassword;

    /** http headers to call stream load api */
    private Map<String,String> httpHeaders;

    /** A buffer's capacity, in bytes.  */
    private int bufferSize;

    /** BufferSize * BufferCount is the max capacity to buffer data before doing real stream load */
    private int bufferCount;

    public String getFeHost() {
        return feHost;
    }

    public void setFeHost(String feHost) {
        this.feHost = feHost;
    }

    public String getFeHttpPort() {
        return feHttpPort;
    }

    public void setFeHttpPort(String feHttpPort) {
        this.feHttpPort = feHttpPort;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getLoginUser() {
        return loginUser;
    }

    public void setLoginUser(String loginUser) {
        this.loginUser = loginUser;
    }

    public String getLoginPassword() {
        return loginPassword;
    }

    public void setLoginPassword(String loginPassword) {
        this.loginPassword = loginPassword;
    }

    public Map<String, String> getHttpHeaders() {
        return httpHeaders;
    }

    public void setHttpHeaders(Map<String, String> httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getBufferCount() {
        return bufferCount;
    }

    public void setBufferCount(int bufferCount) {
        this.bufferCount = bufferCount;
    }


}
