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

import java.io.IOException;
import java.io.InputStream;

/**
 * Record Stream for writing record.
 */
public class RecordStream extends InputStream {
    /** buffer data before stream load */
    private final RecordBuffer recordBuffer;

    /**
     * Construct method
     * @param bufferSize A buffer's capacity, in bytes.
     * @param bufferCount BufferSize * BufferCount is the max capacity to buffer data before doing real stream load
     */
    public RecordStream(int bufferSize, int bufferCount) {
        this.recordBuffer = new RecordBuffer(bufferSize, bufferCount);
    }

    /**
     * This method will be called at the beginning of each stream load batch
     */
    public void startInput() {
        recordBuffer.startBufferData();
    }

    /**
     * write data into buffer for a specified stream load batch
     * @param buff
     * @throws IOException
     */
    public void write(byte[] buff) throws IOException {
        try {
            recordBuffer.write(buff);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * if true then could write into buffer successfully
     * @param writeLength
     * @return
     */
    public boolean canWrite(long writeLength) {
        return recordBuffer.canWrite(writeLength);
    }

    /***
     * read data from buffer into buff variable
     * @param buff
     * @param start the start position in buff
     * @param length
     * @return the number in bytes to read
     * @throws IOException
     */
    @Override
    public int read(byte[] buff, int start, int length) throws IOException {
        try {
            return recordBuffer.read(buff, start, length);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read() throws IOException {
        return -1;
    }

    @Override
    public int read(byte[] buff) throws IOException {
        try {
            return recordBuffer.read(buff, 0, buff.length);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * This method will be called at the end of each stream load batch
     */
    public void endInput() throws InterruptedException{
        recordBuffer.stopBufferData();
    }

    /**
     * Get recordBuffer's writting length in bytes
     * @return
     */
    public long getWriteLength() {
        return recordBuffer.getWriteLength();
    }

    /**
     * Support unit test
     */
    public void clearRecordStream() throws InterruptedException {
        recordBuffer.clearRecordBuffer();
    }
}
