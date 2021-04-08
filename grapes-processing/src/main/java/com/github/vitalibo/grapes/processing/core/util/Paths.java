package com.github.vitalibo.grapes.processing.core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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

}
