package org.apache.hop.vfs.gs;

import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;

@VfsPlugin(
  type = "googledrive",
  typeDescription = "Google Drive VFS"
)
public class GoogleDriveVfsPlugin implements IVfs {
  @Override public String[] getUrlSchemes() {
    return new String[ ] { "googledrive" };
  }

  @Override public FileProvider getProvider() {
    return new GoogleDriveFileProvider();
  }
}
