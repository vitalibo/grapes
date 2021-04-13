package com.github.vitalibo.grapes.visualization;

import com.github.vitalibo.grapes.visualization.view.GraphView;
import prefuse.data.io.DataIOException;
import prefuse.data.io.GraphMLReader;

import javax.swing.*;

public class Application {

    public static void main(String[] args) throws DataIOException {
        JFrame frame = new JFrame("Grapes | Six Degrees of Separation");
        frame.setContentPane(new GraphView(new GraphMLReader().readGraph(args[0])));
        frame.pack();
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

}
