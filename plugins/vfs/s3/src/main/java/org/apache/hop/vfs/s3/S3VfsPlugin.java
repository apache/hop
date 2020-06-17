package org.apache.hop.vfs.s3;

import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;
import org.apache.hop.vfs.s3.s3.vfs.S3FileProvider;

@VfsPlugin(
  type = "s3",
  typeDescription = "S3 VFS plugin"
)
public class S3VfsPlugin implements IVfs {
  @Override public String[] getUrlSchemes() {
    return new String[] { "s3" };
  }

  @Override public FileProvider getProvider() {
    return new S3FileProvider();
  }
}
