package com.github.vitalibo.grapes.processing.core.io.model;

import com.github.vitalibo.grapes.processing.TestHelper;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class MultipleInputWritableTest {

    @Test
    public void testArrayPrimitiveWritableSerDe() throws IOException {
        MultipleInputWritable<ArrayPrimitiveWritable> original =
            new MultipleInputWritable.ArrayPrimitiveWritable((byte) 123)
                .wrap(new ArrayPrimitiveWritable(new int[]{1, 2, 3, 4, 5}));

        MultipleInputWritable<ArrayPrimitiveWritable> actual =
            TestHelper.serDe(original, new MultipleInputWritable.ArrayPrimitiveWritable());

        Assert.assertNotSame(actual, original);
        Assert.assertNotSame(actual.getInstance(), original.getInstance());
        Assert.assertEquals(actual.getInstance().get(), new int[]{1, 2, 3, 4, 5});
        Assert.assertEquals(actual.getType(), (byte) 123);
    }

}