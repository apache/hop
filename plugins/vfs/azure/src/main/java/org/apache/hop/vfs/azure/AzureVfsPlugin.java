package org.apache.hop.vfs.azure;

import com.sshtools.vfs.azure.AzureFileProvider;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;

@VfsPlugin(
  type = "azfs",
  typeDescription = "Azure VFS plugin"
)
public class AzureVfsPlugin implements IVfs {
  @Override public String[] getUrlSchemes() {
    return new String[] { "azfs" };
  }

  @Override public FileProvider getProvider() {
    AzureFileProvider fileProvider = new AzureFileProvider();
    return fileProvider;
  }
}
