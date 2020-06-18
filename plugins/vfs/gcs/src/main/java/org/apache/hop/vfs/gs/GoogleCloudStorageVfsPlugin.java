package org.apache.hop.vfs.gs;

import com.sshtools.vfs.gcs.GoogleStorageFileProvider;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;

@VfsPlugin(
  type = "gcs",
  typeDescription = "Google Cloud Storage VFS plugin"
)
public class GoogleCloudStorageVfsPlugin implements IVfs {
  @Override public String[] getUrlSchemes() {
    return new String[] { "gcs" };
  }

  @Override public FileProvider getProvider() {
    return new GoogleStorageFileProvider();
  }
}
