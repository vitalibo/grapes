package com.github.vitalibo.grapes.processing.infrastructure.conf;

import com.github.vitalibo.grapes.processing.TestHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

public class HoconConfigurationTest {

    @Test
    public void testParseHocon() {
        Config config = ConfigFactory.parseResources(TestHelper.resourcePath("config.hocon").substring(1));

        Configuration actual = HoconConfiguration.parseHocon(config, "test");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.get("x"), "y");
        Assert.assertEquals(actual.get("y"), "z");
        Assert.assertEquals(actual.get("z"), "none");
        Assert.assertEquals(actual.get("app.foo"), "bar");
        Assert.assertEquals(actual.getInt("app.int", -1), 1);
        Assert.assertEquals(actual.getDouble("app.double", -1), 1.23);
        Assert.assertEquals(actual.getBoolean("app.bool", false), true);
        Assert.assertEquals(actual.getFloat("app.number", -1f), 9.87f);
        Assert.assertEquals(actual.getStrings("app.arr"), new String[]{"arr1", "arr2", "arr3"});
        Assert.assertEquals(actual.getInts("app.arr2"), new int[]{11, 22, 33});
        Assert.assertEquals(actual.get("app.properties.f4"), "com.sun.java.accessibility.util.AWTEventMonitor");
        Map<String, String> properties = actual.getPropsWithPrefix("app.properties.");
        Assert.assertEquals(properties.get("f1.f2.f3"), "com.apple.eio.FileManager");
        Assert.assertEquals(properties.get("f4"), "com.sun.java.accessibility.util.AWTEventMonitor");
        Assert.assertEquals(properties.get("f0"), "com.ctc.wstx.api.WstxInputProperties");
    }

}
