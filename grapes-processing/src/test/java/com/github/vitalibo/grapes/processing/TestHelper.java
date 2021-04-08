package com.github.vitalibo.grapes.processing;

import lombok.SneakyThrows;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public final class TestHelper {

    private TestHelper() {
    }

    public static String resourceAsString(String resource) {
        return new BufferedReader(new InputStreamReader(resourceAsInputStream(resource)))
            .lines()
            .collect(Collectors.joining(System.lineSeparator()));
    }

    public static InputStream resourceAsInputStream(String resource) {
        InputStream stream = TestHelper.class.getResourceAsStream(resource);
        Objects.requireNonNull(stream, String.format("Resource do not exists. '%s'", resource));
        return stream;
    }

    @SneakyThrows
    public static File resourceAsFile(String resource) {
        URL url = TestHelper.class.getResource(resource);
        Objects.requireNonNull(url, String.format("Resource do not exists. '%s'", resource));
        return new File(url.toURI());
    }

    public static String resourcePath(String resource) {
        return resourcePath(resource, 3);
    }

    public static String resourcePath(String resource, int n) {
        final StackTraceElement stack = Thread.currentThread().getStackTrace()[n];
        return File.separator +
            String.join(File.separator,
                stack.getClassName().replace('.', File.separatorChar),
                stack.getMethodName(),
                resource);
    }

    public static <T extends Writable> T serDe(T original, T instance) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        original.write(new DataOutputStream(baos));
        instance.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        return instance;
    }

    public static List<Map.Entry<Integer, int[]>> resourceAsListPair(String path) {
        return new BufferedReader(new InputStreamReader(resourceAsInputStream(path)))
            .lines()
            .map(line -> line.split(" - ", 2))
            .map(split -> new AbstractMap.SimpleEntry<>(
                Integer.parseInt(split[0].trim()),
                Arrays.stream(split[1].split(" "))
                    .filter(i -> !i.isEmpty())
                    .mapToInt(Integer::parseInt)
                    .toArray()))
            .collect(Collectors.toList());
    }

}
