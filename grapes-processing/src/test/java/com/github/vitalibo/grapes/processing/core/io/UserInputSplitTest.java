package com.github.vitalibo.grapes.processing.core.io;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;

public class UserInputSplitTest {

    @Test
    public void testNoArgsConstructor() {
        new UserInputSplit();
    }

    @Test
    public void testGetLength() {
        UserInputSplit split = new UserInputSplit(2, new int[]{1, 2, 3, 4});

        Assert.assertEquals(split.getLength(), 4);
    }

    @Test
    public void testSerDe() throws IOException {
        UserInputSplit original = new UserInputSplit(2, new int[]{1, 2, 3, 4});

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        original.write(new DataOutputStream(baos));
        UserInputSplit actual = new UserInputSplit();
        actual.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

        Assert.assertEquals(actual.getId(), 2);
        Assert.assertEquals(actual.getUsers(), new int[]{1, 2, 3, 4});
    }

}
