package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.ui.hopgui.partition.PartitionSettings;
import org.eclipse.swt.widgets.Shell;

public interface IPartitionSchemaSelection {
  /**
   * Perform the schema selection.  Return a non-null string if a selection happened (in a dialog)
   *
   * @param shell
   * @param partitionSettings
   * @return
   * @throws HopException
   */
  public String schemaFieldSelection( Shell shell, PartitionSettings partitionSettings ) throws HopException;
}
