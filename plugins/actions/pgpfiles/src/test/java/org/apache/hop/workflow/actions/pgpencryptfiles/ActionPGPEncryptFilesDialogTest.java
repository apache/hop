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
 */

package org.apache.hop.workflow.actions.pgpencryptfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.function.Consumer;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.finders.UIThreadRunnable;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The grid on the General tab is the only place a user picks the key to encrypt or sign with, so
 * every column has to end up in the field its header names. Between 2.18.0 and 2.18.1 it did not:
 * the dialog filled the grid in column order (action, source, wildcard, user id, destination) but
 * read it back as user id, destination, wildcard. Opening a working action and pressing OK was
 * enough to move the wildcard into the User ID and the key into the destination path, which broke
 * signing for anyone who edited an existing workflow in the GUI. See
 * https://github.com/apache/hop/issues/7276 and https://github.com/apache/hop/issues/8206.
 */
@Tag("uitest")
class ActionPGPEncryptFilesDialogTest extends SwtBotTestBase {

  private static final Class<?> PKG = ActionPGPEncryptFiles.class;
  private static final String DIALOG_TITLE =
      BaseMessages.getString(PKG, "ActionPGPEncryptFiles.Title");

  /** Deliberately distinguishable: a rotation between the three text columns cannot pass. */
  private static final String SIGN_SOURCE = "/data/outbox/invoices.csv";

  private static final String SIGN_WILDCARD = ".*\\.csv$";
  private static final String SIGN_USER_ID = "signing-key@example.org";
  private static final String SIGN_DESTINATION = "/data/signed/invoices.csv.asc";

  private static final String SEAL_SOURCE = "/data/outbox/payments.xml";
  private static final String SEAL_WILDCARD = ".*\\.xml$";
  private static final String SEAL_USER_ID = "partner-key@example.com";
  private static final String SEAL_DESTINATION = "/data/sealed/payments.xml.gpg";

  @Test
  void openingAndConfirmingTheDialogLeavesEveryRowUntouched() {
    ActionPGPEncryptFiles action = actionWithTwoRows();

    openAndConfirm(action, bot -> {});

    assertEquals(2, action.getPgpFiles().size(), "row count");

    ActionPGPEncryptFiles.PgpFile signRow = action.getPgpFiles().get(0);
    assertEquals(ActionPGPEncryptFiles.ActionType.SIGN, signRow.getActionType(), "action type");
    assertEquals(SIGN_SOURCE, signRow.getSourceFileFolder(), "source file/folder");
    assertEquals(SIGN_WILDCARD, signRow.getWildcard(), "wildcard");
    assertEquals(SIGN_USER_ID, signRow.getUserId(), "user id (the key to sign with)");
    assertEquals(SIGN_DESTINATION, signRow.getDestinationFileFolder(), "destination file/folder");

    ActionPGPEncryptFiles.PgpFile sealRow = action.getPgpFiles().get(1);
    assertEquals(
        ActionPGPEncryptFiles.ActionType.SIGN_AND_ENCRYPT, sealRow.getActionType(), "action type");
    assertEquals(SEAL_SOURCE, sealRow.getSourceFileFolder(), "source file/folder");
    assertEquals(SEAL_WILDCARD, sealRow.getWildcard(), "wildcard");
    assertEquals(SEAL_USER_ID, sealRow.getUserId(), "user id (the key to sign with)");
    assertEquals(SEAL_DESTINATION, sealRow.getDestinationFileFolder(), "destination file/folder");
  }

  @Test
  void everyGridColumnIsStoredInTheFieldItsHeaderNames() {
    ActionPGPEncryptFiles action = actionWithTwoRows();

    openAndConfirm(
        action,
        bot -> {
          SWTBotTable grid = bot.table();
          List<String> headers = grid.columns();

          // Type a marker naming the column into the first row, under the header itself, so the
          // assertions below read as "what the user typed under 'User ID' is the user id".
          typeInto(grid, 0, headers.indexOf(label("SourceFileFolder")), "typed-under-source");
          typeInto(grid, 0, headers.indexOf(label("Wildcard")), "typed-under-wildcard");
          typeInto(grid, 0, headers.indexOf(label("UserID")), "typed-under-user-id");
          typeInto(grid, 0, headers.indexOf(label("DestinationFileFolder")), "typed-under-dest");
        });

    ActionPGPEncryptFiles.PgpFile row = action.getPgpFiles().get(0);
    assertEquals("typed-under-source", row.getSourceFileFolder(), label("SourceFileFolder"));
    assertEquals("typed-under-wildcard", row.getWildcard(), label("Wildcard"));
    assertEquals("typed-under-user-id", row.getUserId(), label("UserID"));
    assertEquals(
        "typed-under-dest", row.getDestinationFileFolder(), label("DestinationFileFolder"));
  }

  /** Opens the dialog, runs {@code interactions} against it, then presses OK. */
  private void openAndConfirm(ActionPGPEncryptFiles action, Consumer<SWTBot> interactions) {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    withDialog(
        parent ->
            new ActionPGPEncryptFilesDialog(parent, action, workflowMeta, new Variables()).open(),
        bot -> {
          SWTBot dialog = bot.shell(DIALOG_TITLE).activate().bot();
          interactions.accept(dialog);
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
  }

  /**
   * Writes a cell the way the dialog itself fills the grid. SWTBot drives the in-place editors of a
   * plain SWT table; Hop's TableView builds its own, so the value goes straight onto the item.
   */
  private static void typeInto(SWTBotTable grid, int row, int column, String value) {
    Table table = grid.widget;
    UIThreadRunnable.syncExec(() -> table.getItem(row).setText(column, value));
  }

  private static String label(String field) {
    return BaseMessages.getString(PKG, "ActionPGPEncryptFiles.Fields." + field + ".Label");
  }

  private static ActionPGPEncryptFiles actionWithTwoRows() {
    ActionPGPEncryptFiles action = new ActionPGPEncryptFiles("PGP encrypt files");
    action
        .getPgpFiles()
        .add(
            pgpFile(
                ActionPGPEncryptFiles.ActionType.SIGN,
                SIGN_SOURCE,
                SIGN_WILDCARD,
                SIGN_USER_ID,
                SIGN_DESTINATION));
    action
        .getPgpFiles()
        .add(
            pgpFile(
                ActionPGPEncryptFiles.ActionType.SIGN_AND_ENCRYPT,
                SEAL_SOURCE,
                SEAL_WILDCARD,
                SEAL_USER_ID,
                SEAL_DESTINATION));
    return action;
  }

  private static ActionPGPEncryptFiles.PgpFile pgpFile(
      ActionPGPEncryptFiles.ActionType actionType,
      String source,
      String wildcard,
      String userId,
      String destination) {
    ActionPGPEncryptFiles.PgpFile file = new ActionPGPEncryptFiles.PgpFile();
    file.setActionType(actionType);
    file.setSourceFileFolder(source);
    file.setWildcard(wildcard);
    file.setUserId(userId);
    file.setDestinationFileFolder(destination);
    return file;
  }
}
