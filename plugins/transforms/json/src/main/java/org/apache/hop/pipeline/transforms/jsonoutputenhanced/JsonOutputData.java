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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.Writer;

public class JsonOutputData extends BaseTransformData implements ITransformData {

    public IRowMeta inputRowMeta;
    public IRowMeta outputRowMeta;
    public int inputRowMetaSize;
    public boolean rowsAreSafe;

    public int nrFields;
    public int[] fieldIndexes;
    public int[] keysGroupIndexes;
    public int nrRow;

    private boolean outputValue;
    private boolean writeToFile;
    private boolean genFlat;
    private boolean genLoopOverKey;

    public String realBlocName;
    public int splitnr;
    public Writer writer;

    /**
     *
     */
    public JsonOutputData() {
        super();

        this.nrRow = 0;
        this.outputValue = false;
        this.writeToFile = false;
        this.genFlat = false;
        this.genLoopOverKey = false;
        this.writer = null;
    }

    public boolean isGenFlat() {
        return genFlat;
    }

    public void setGenFlat(boolean genFlat) {
        this.genFlat = genFlat;
    }

    public boolean isGenLoopOverKey() {
        return genLoopOverKey;
    }

    public void setGenLoopOverKey(boolean genLoopOverKey) {
        this.genLoopOverKey = genLoopOverKey;
    }

    public boolean isOutputValue() {
        return outputValue;
    }

    public void setOutputValue(boolean outputValue) {
        this.outputValue = outputValue;
    }

    public boolean isWriteToFile() {
        return writeToFile;
    }

    public void setWriteToFile(boolean writeToFile) {
        this.writeToFile = writeToFile;
    }
}
