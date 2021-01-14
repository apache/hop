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
package org.apache.hop.core.util;

import org.apache.hop.core.exception.HopException;

import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Utility class for socket related methods
 */
public class SocketUtil {

  private SocketUtil() {
  }

  /**
   * Attempts to connect to the specified host, wrapping any exceptions in a HopException
   *
   * @param host    the host to connect to
   * @param port    the port to connect to
   * @param timeout the timeout
   * @throws HopException
   */
  public static void connectToHost( String host, int port, int timeout ) throws HopException {

    try ( Socket socket = new Socket() ) {
      InetSocketAddress is = new InetSocketAddress( host, port );
      if ( timeout < 0 ) {
        socket.connect( is );
      } else {
        socket.connect( is, timeout );
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }
}
