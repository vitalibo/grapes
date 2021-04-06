package com.github.vitalibo.grapes.processing.core.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.function.Function;

import static org.testng.Assert.*;

public class ThrowingTest {

    @Test
    public void testFunction() {
        Function<Object, Object> function = Throwing.function((x) -> {
            throw new Exception("foo");
        });

        Exception actual = expectThrows(Exception.class, () -> function.apply(null));

        Assert.assertEquals(actual.getMessage(), "foo");
    }

}
