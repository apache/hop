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
package org.apache.hop.vfs.smb;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.connection.LeaseManager;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/** Close order, without a live SMB server. Directory leases have to go first. */
class SmbjSmbShareTest {

  @Test
  void closeReleasesDirectoryLeasesWhileTheShareIsOpen() throws Exception {
    SMBClient client = mock(SMBClient.class);
    SmbjSmbShare share = share(client);
    DiskShare disk = mock(DiskShare.class);
    Session session = mock(Session.class);
    Connection connection = mock(Connection.class);
    LeaseManager leases = mock(LeaseManager.class);
    AtomicBoolean shareOpen = new AtomicBoolean(true);
    when(disk.isConnected()).thenAnswer(invocation -> shareOpen.get());
    doAnswer(
            invocation -> {
              shareOpen.set(false);
              return null;
            })
        .when(disk)
        .close();
    when(connection.getLeaseManager()).thenReturn(leases);
    doAnswer(
            invocation -> {
              assertTrue(shareOpen.get());
              return null;
            })
        .when(leases)
        .close();
    bind(share, connection, session, disk);

    share.close();

    InOrder order = inOrder(connection, leases, disk, session);
    order.verify(connection).getLeaseManager();
    order.verify(leases).close();
    order.verify(disk).close();
    order.verify(session).close();
    order.verify(connection).close();
    verify(client).close();
  }

  @Test
  void reconnectDropsTheOldSessionBeforeOpeningAnother() throws Exception {
    SMBClient client = mock(SMBClient.class);
    when(client.connect(anyString(), anyInt())).thenThrow(new IOException("refused"));
    SmbjSmbShare share = share(client);
    DiskShare disk = mock(DiskShare.class);
    Session session = mock(Session.class);
    Connection connection = mock(Connection.class);
    LeaseManager leases = mock(LeaseManager.class);
    when(disk.isConnected()).thenReturn(false);
    when(connection.getLeaseManager()).thenReturn(leases);
    bind(share, connection, session, disk);

    assertThrows(IOException.class, () -> share.children(""));

    InOrder order = inOrder(disk, session, connection, client);
    order.verify(disk).close();
    order.verify(session).close();
    order.verify(connection).close();
    order.verify(client).connect("files.example", 445);
    verify(leases, never()).close();
  }

  private static SmbjSmbShare share(SMBClient client) {
    return new SmbjSmbShare(
        client,
        "files.example",
        445,
        "data",
        "CORP",
        "alice",
        new AuthenticationContext("alice", "secret".toCharArray(), "CORP"));
  }

  private static void bind(
      SmbjSmbShare share, Connection connection, Session session, DiskShare disk) throws Exception {
    set(share, "connection", connection);
    set(share, "session", session);
    set(share, "disk", disk);
  }

  private static void set(SmbjSmbShare share, String name, Object value) throws Exception {
    Field field = SmbjSmbShare.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(share, value);
  }
}
