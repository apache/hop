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
 *
 */
 
package org.apache.hop.pipeline.transforms.dorisbulkloader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.apache.hop.core.encryption.Encr;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class DorisStreamLoad {
    private static final byte[] JSON_ARRAY_START = LoadConstants.JSON_ARRAY_START.getBytes(StandardCharsets.UTF_8);
    private static final byte[] JSON_ARRAY_END = LoadConstants.JSON_ARRAY_END.getBytes(StandardCharsets.UTF_8);
    /** used to serialize or deserialize json string */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** doris stream load http url */
    private String loadUrl;

    /** doris stream load http basic auth user */
    private String loginUser;

    /** doris stream load http basic auth password */
    private String loginPassword;

    /** doris stream load http request headers */
    private Map<String,String> httpHeaders;

    /** stream load format */
    private String format;

    /** stream load data line delimiter */
    private final byte[] lineDelimiter;

    private CloseableHttpClient httpClient;

    /** buffered data */
    private final RecordStream recordStream;

    /** used to indicate whether it is the first record in current batch */
    private boolean loadBatchFirstRecord;

    public DorisStreamLoad(StreamLoadProperty streamLoadProperty) {
        this.loadUrl = String.format(LoadConstants.LOAD_URL_PATTERN,
                streamLoadProperty.getFeHost(),
                streamLoadProperty.getFeHttpPort(),
                streamLoadProperty.getDatabaseName(),
                streamLoadProperty.getTableName());
        this.loginUser = streamLoadProperty.getLoginUser();
        this.loginPassword = streamLoadProperty.getLoginPassword();
        this.httpHeaders = streamLoadProperty.getHttpHeaders();
        this.format = httpHeaders.get(LoadConstants.FORMAT_KEY);
        if (LoadConstants.JSON.equals(this.format)) {
            this.lineDelimiter = LoadConstants.LINE_DELIMITER_JSON.getBytes();
        } else {
            this.lineDelimiter = httpHeaders.get(LoadConstants.LINE_DELIMITER_KEY).getBytes();
        }
        this.recordStream = new RecordStream(streamLoadProperty.getBufferSize(), streamLoadProperty.getBufferCount());
        this.loadBatchFirstRecord = true;
    }

    /**
     * start to write data into buffer
     * @throws IOException
     */
    public void startWritingIntoBuffer() throws IOException{
        loadBatchFirstRecord = true;
        recordStream.startInput();
        if (LoadConstants.JSON.equals(format)) {
            recordStream.write(JSON_ARRAY_START);
        }
    }

    /**
     * write record into buffer.
     * @param record
     * @throws IOException
     */
    public void writeRecord(byte[] record) throws IOException{
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            recordStream.write(lineDelimiter);
        }
        recordStream.write(record);
    }

    /**
     * if true then could write into buffer successfully
     * @param writeLength
     * @return
     */
    public boolean canWrite(long writeLength) {
        if (LoadConstants.JSON.equals(this.format)) {
            writeLength++;
        }

        return recordStream.canWrite(writeLength);
    }

    /**
     * stop to load buffer data into doris by http api
     * @return
     * @throws IOException
     */
    public void endWritingIntoBuffer() throws IOException, InterruptedException {
        if (LoadConstants.JSON.equals(format)) {
            recordStream.write(JSON_ARRAY_END);
        }
        recordStream.endInput();
    }

    /**
     * call doris stream load http api
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws DorisStreamLoadException
     */
    public ResponseContent executeDorisStreamLoad() throws IOException, DorisStreamLoadException {
        HttpPut put = new HttpPut(loadUrl);
        put.setHeader(HttpHeaders.EXPECT, LoadConstants.EXCEPT_DEFAULT);
        put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(loginUser, loginPassword));
        put.setHeader(LoadConstants.LABEL_KEY, LoadConstants.LABEL_SUFFIX + UUID.randomUUID().toString());
        //put.setHeader("Content-Type", "text/plain; charset=UTF-8");
        if (LoadConstants.JSON.equals(format)) {
            put.setHeader(LoadConstants.STRIP_OUTER_ARRAY_KEY, LoadConstants.STRIP_OUTER_ARRAY_DEFAULT);
        }
        httpHeaders.forEach(put::setHeader);

        InputStreamEntity entity = new InputStreamEntity(recordStream, recordStream.getWriteLength());
        entity.setChunked(false);
        put.setEntity(entity);

        if (httpClient == null) {
            httpClient = HttpClients
                    .custom()
                    .setRedirectStrategy(new DefaultRedirectStrategy() {
                        @Override
                        protected boolean isRedirectable(String method) {
                            // If the connection target is FE, you need to deal with 307 redirect。
                            return true;
                        }
                    }).build();
        }

        CloseableHttpResponse response = httpClient.execute(put);
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 && response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            return OBJECT_MAPPER.readValue(loadResult, ResponseContent.class);
        } else {
            throw new DorisStreamLoadException("stream load error: " + response.getStatusLine().toString());
        }
    }

    /**
     * support unit test
     */
    public void clearRecordStream() throws InterruptedException {
        recordStream.clearRecordStream();
    }

    public void close() throws IOException {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                throw new IOException("Closing httpClient failed.", e);
            }
        }
    }

    /**
     * Get http basic Auth header
     * @param username
     * @param password
     * @return
     */
    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + Encr.decryptPasswordOptionallyEncrypted(password);
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }
}
