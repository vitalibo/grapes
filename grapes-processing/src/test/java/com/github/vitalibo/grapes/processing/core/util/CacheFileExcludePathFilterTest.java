package com.github.vitalibo.grapes.processing.core.util;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CacheFileExcludePathFilterTest {

    private CacheFileExcludePathFilter filter;

    @BeforeMethod
    public void setUp() {
        filter = new CacheFileExcludePathFilter();
    }

    @DataProvider
    public Object[][] samples() {
        return new Object[][]{
            {"/foo/part-m-00000", true}, {"/foo/part-r-00001", true}, {"/foo/foo.txt", true}, {"/foo/", true},
            {"/foo/cache-m-12345", false}, {"/foo/cache-r-54321", false}, {"/foo/cache.txt", true}
        };
    }

    @Test(dataProvider = "samples")
    public void testAccept(String path, boolean expected) {
        boolean actual = filter.accept(new Path(path));

        Assert.assertEquals(actual, expected);
    }

}
