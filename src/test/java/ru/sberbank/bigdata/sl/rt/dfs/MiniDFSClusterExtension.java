package ru.sberbank.bigdata.sl.rt.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import ru.sberbank.bigdata.sl.rt.BaseTransformationTest;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

// Идея позаимствована из ответа https://stackoverflow.com/a/65897949
public class MiniDFSClusterExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static final Lock LOCK = new ReentrantLock();
    private static volatile boolean started = false;

    private static Logger logger = Logger.getLogger(String.valueOf(MiniDFSClusterExtension.class));
    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        LOCK.lock();
        try {
            if (!started) {
                started = true;

                // Регистрирует callback hook, вызываемый, когда root test context гасится.
                context.getRoot().getStore(GLOBAL).put("miniDfsCluster", this);

                startDfsMiniCluster();
            }
        } finally {
            LOCK.unlock();
        }
    }

    @Override
    public void close() {
        hdfsCluster.shutdown(true, true);
        FileUtil.fullyDelete(baseDir);
    }

    public static DistributedFileSystem getFileSystem() throws IOException {
        return hdfsCluster.getFileSystem();
    }

    public static String getHdfsUri() {
        return "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
    }

    private void startDfsMiniCluster() throws IOException {
        logger.info("Starting DFS MiniCluster");
        Configuration conf = new Configuration();
        baseDir = new File(PathUtils.getTestDir(BaseTransformationTest.class), "miniHDFS");
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        hdfsCluster = new MiniDFSCluster.Builder(conf).
                manageNameDfsDirs(true).
                manageDataDfsDirs(true).
                numDataNodes(3).
                build();
        hdfsCluster.waitActive();
        logger.info("DFS MiniCluster started");
    }
}
