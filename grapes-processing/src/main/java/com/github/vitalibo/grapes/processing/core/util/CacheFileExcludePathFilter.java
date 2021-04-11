package com.github.vitalibo.grapes.processing.core.util;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.regex.Pattern;

@RequiredArgsConstructor
public class CacheFileExcludePathFilter implements PathFilter {

    private final Pattern pattern;

    public CacheFileExcludePathFilter() {
        this(Pattern.compile("cache-[rm]-[0-9]{5}"));
    }

    @Override
    public boolean accept(Path path) {
        return !pattern.matcher(path.getName()).matches();
    }

}
