package com.github.vitalibo.grapes.processing.core.util;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class Paths {

    private Paths() {
    }

    public static Path createTempDirectory(Configuration configuration) {
        return Paths.createTempDirectory(
            configuration.get("hadoop.tmp.dir"));
    }

    public static Path createTempDirectory(String prefix) {
        return new Path((prefix.endsWith("/") ? prefix : prefix + "/")
            + Long.toString(Double.doubleToLongBits(Math.random()), 36));
    }

    public static void createTextFile(FileSystem fs, String path, String content) throws IOException {
        FSDataOutputStream stream = fs.create(new Path(path));  // NOPMD
        stream.write(content.getBytes());
        stream.close();
    }

    public static List<Path> listFiles(FileSystem fs, String path, boolean recursive) throws IOException {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                new Iterator<Path>() {
                    final RemoteIterator<LocatedFileStatus> delegate =
                        fs.listFiles(new Path(path), recursive);

                    @Override
                    @SneakyThrows
                    public boolean hasNext() {
                        return delegate.hasNext();
                    }

                    @Override
                    @SneakyThrows
                    public Path next() {
                        return delegate.next().getPath();
                    }
                }, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
    }

    public static Stream<String> cacheFilesAsTextLines(URI[] uris) {
        return Optional.ofNullable(uris)
            .map(Arrays::stream)
            .orElse(Stream.empty())
            .map(patternsURI -> new Path(patternsURI.getPath()))
            .map(Path::getName)
            .flatMap(Throwing.function(name -> new BufferedReader(new FileReader(name))
                .lines()));
    }

}
