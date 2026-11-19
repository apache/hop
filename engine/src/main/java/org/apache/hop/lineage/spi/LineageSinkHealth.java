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

package org.apache.hop.lineage.spi;

/**
 * How a lineage sink's delivery is going, for an operator: what it sent, what it couldn't, what
 * waits, and the last thing that went wrong.
 *
 * @param sent events the backend accepted
 * @param failed delivery attempts that failed and are, or were, tried again
 * @param rejected events the backend refused; they are not sent again
 * @param dropped events dropped because the sink's queue was full
 * @param waiting events handed to the sink and not delivered yet, in memory or on disk
 * @param spooled events kept on disk until they are delivered; -1 without a spool
 * @param lastSentMillis when the backend last accepted an event; 0 when never
 * @param lastErrorMillis when the last error happened; 0 when never
 * @param lastError what the last error was, or null
 */
public record LineageSinkHealth(
    long sent,
    long failed,
    long rejected,
    long dropped,
    long waiting,
    long spooled,
    long lastSentMillis,
    long lastErrorMillis,
    String lastError) {}
