package com.github.vitalibo.grapes.processing;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        public <K, V> SequenceFile.Writer createSequenceFile(String path,
                                                             Class<K> keyCls, Class<V> valueCls) throws IOException {
            return SequenceFile.createWriter(
                fs.getConf(),
                SequenceFile.Writer.file(new Path(path)),
                SequenceFile.Writer.keyClass(keyCls),
                SequenceFile.Writer.valueClass(valueCls));
        }

        public void createSequenceFile(String path) throws IOException {
            final IntWritable key = new IntWritable();
            final ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
            SequenceFile.Writer writer = createSequenceFile("/" + path, IntWritable.class, ArrayPrimitiveWritable.class);
            List<Map.Entry<Integer, int[]>> entries = TestHelper.resourceAsListPair(
                TestHelper.resourcePath(path + ".txt", 3));
            for (Map.Entry<Integer, int[]> entry : entries) {
                key.set(entry.getKey());
                value.set(entry.getValue());
                writer.append(key, value);
            }

            writer.hflush();
            writer.close();
        }

        public void createTextFile(String target) throws IOException {
            create("/" + target,
                TestHelper.resourceAsString(
                    TestHelper.resourcePath(target + ".txt", 3)));
        }

        public void assertEqualsSequenceFile(String path) throws IOException {
            final IntWritable key = new IntWritable();
            final ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
            SequenceFile.Reader reader = openSequenceFile("/" + path);
            List<Map.Entry<Integer, int[]>> expected = TestHelper.resourceAsListPair(
                TestHelper.resourcePath(path + ".txt", 3));
            Map<Integer, int[]> actual = new HashMap<>();
            while (reader.next(key, value)) {
                actual.put(key.get(), (int[]) value.get());
            }

            Assert.assertEquals(actual.size(), expected.size());
            for (Map.Entry<Integer, int[]> entry : expected) {
                Assert.assertEquals(actual.get(entry.getKey()), entry.getValue());
            }
        }

        public SequenceFile.Reader openSequenceFile(String path) throws IOException {
            return new SequenceFile.Reader(
                fs.getConf(),
                SequenceFile.Reader.file(new Path(path)));
        }

    }

}
