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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

public class PdOption {
    private boolean isValidating;
    private boolean useUrl;
    private boolean useSnippet;
    private String encoding;
    private boolean isXmlSourceFile;
    private String loopXPath;

    PdOption() {
        isValidating = false;
        useUrl = false;
        useSnippet = false;
        encoding = null;
        isXmlSourceFile = false;
        loopXPath = "";
    }

    public void setValidating(boolean validating) {
        isValidating = validating;
    }

    public void setUseUrl(boolean useUrl) {
        this.useUrl = useUrl;
    }

    public void setUseSnippet(boolean useSnippet) {
        this.useSnippet = useSnippet;
    }

    //if the encoding is not null, the source must be a file
    public void setEncoding(String encoding) {
        this.encoding = encoding;
        this.isXmlSourceFile=true;
    }

    public boolean isValidating() {
        return isValidating;
    }

    public boolean isUseUrl() {
        return useUrl;
    }

    public boolean isUseSnippet() {
        return useSnippet;
    }

    public String getEncoding() {
        return encoding;
    }

    public boolean isXmlSourceIsFile() {
        return isXmlSourceFile;
    }

    public String getLoopXPath() {
        return loopXPath;
    }

    public void setLoopXPath(String loopXPath) {
        this.loopXPath = loopXPath;
    }
}
