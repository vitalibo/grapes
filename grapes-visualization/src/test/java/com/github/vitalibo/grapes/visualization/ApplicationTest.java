package com.github.vitalibo.grapes.visualization;

import org.testng.annotations.Test;

public class ApplicationTest {

    @Test
    public void testApp() throws Exception {
        Application.main(new String[]{ApplicationTest.class.getResource("/part-r-00000.xml").getPath()});
    }

}
