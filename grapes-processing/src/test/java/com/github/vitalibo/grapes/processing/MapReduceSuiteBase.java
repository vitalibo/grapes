package com.github.vitalibo.grapes.processing;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.*;
import java.util.Objects;
import java.util.stream.Collectors;

public class MapReduceSuiteBase {

    protected MiniDFSClusterExt cluster;
    protected FileSystemExt fs;

    @BeforeClass
    public void setUpMiniDFSCluster() throws IOException {
        setUpMiniDFSCluster(new Configuration());
    }

    public void setUpMiniDFSCluster(Configuration configuration) throws IOException {
        cluster = new MiniDFSClusterExt(
            new MiniDFSCluster.Builder(configuration)
                .format(true)
                .numDataNodes(3)
                .build());

        cluster.waitClusterUp();
        fs = new FileSystemExt(cluster.getFileSystem());
    }

    @BeforeMethod
    public void cleanUpFileSystem() throws IOException {
        fs.clean();
    }

    @AfterClass
    public void tearDownMiniDFSCluster() throws IOException {
        if (fs != null) {
            fs.close();
        }

        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @RequiredArgsConstructor
    public static class MiniDFSClusterExt {

        @Delegate
        private final MiniDFSCluster cluster;

        public Configuration getConfiguration() {
            Configuration configuration = new Configuration();
            configuration.addResource(cluster.getConfiguration(0));
            return configuration;
        }

    }

    @RequiredArgsConstructor
    public static class FileSystemExt {

        @Delegate
        private final FileSystem fs;

        public void clean() throws IOException {
            for (FileStatus file : fs.listStatus(new Path("/"))) {
                fs.delete(file.getPath(), true);
            }
        }

        public void sync(String source, String target) throws IOException {
            source = source.endsWith("/") ? source : source + "/";
            target = target.startsWith("/") ? target : "/" + target;
            target = target.endsWith("/") ? target : target + "/";

            File path = TestHelper.resourceAsFile(source);
            String[] files = Objects.requireNonNull(path.list(), String.format("Directory '%s' is empty.", source));
            for (String name : files) {
                create(target + name, TestHelper.resourceAsString(source + name));
            }
        }

        public void create(String path, String content) throws IOException {
            OutputStream outputStream = fs.create(new Path(path));
            outputStream.write(content.getBytes());
            outputStream.close();
        }

        public String open(String path) throws IOException {
            FSDataInputStream fsDataInputStream = fs.open(new Path(path));
            return new BufferedReader(new InputStreamReader(fsDataInputStream.getWrappedStream()))
                .lines()
                .collect(Collectors.joining(System.lineSeparator()));
        }

    }

}
