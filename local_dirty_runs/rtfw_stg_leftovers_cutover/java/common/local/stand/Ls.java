package local.stand;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.net.URI;
import java.text.SimpleDateFormat;

/** Ls <port> <path...>: every file and dir under the paths with mtime (ms), length, HDFS inode id. */
public class Ls {
    public static void main(String[] a) throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:" + a[0]), new Configuration());
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        for (int i = 1; i < a.length; i++) walk(fs, new Path(a[i]), f);
    }
    static void walk(FileSystem fs, Path p, SimpleDateFormat f) throws Exception {
        if (!fs.exists(p)) { System.out.println("MISSING " + p.toUri().getPath()); return; }
        for (FileStatus s : fs.listStatus(p)) {
            long id = s instanceof org.apache.hadoop.hdfs.protocol.HdfsFileStatus ? ((org.apache.hadoop.hdfs.protocol.HdfsFileStatus) s).getFileId() : -1;
            System.out.println((s.isDirectory() ? "D " : "F ") + f.format(s.getModificationTime()) + "\t" + s.getLen() + "\tid=" + id + "\t" + s.getPath().toUri().getPath());
            if (s.isDirectory()) walk(fs, s.getPath(), f);
        }
    }
}
