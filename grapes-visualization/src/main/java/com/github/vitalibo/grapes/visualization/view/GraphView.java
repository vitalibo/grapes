package com.github.vitalibo.grapes.visualization.view;

import prefuse.Display;
import prefuse.Visualization;
import prefuse.action.ActionList;
import prefuse.action.RepaintAction;
import prefuse.action.assignment.ColorAction;
import prefuse.action.layout.graph.ForceDirectedLayout;
import prefuse.activity.Activity;
import prefuse.controls.*;
import prefuse.data.Graph;
import prefuse.render.DefaultRendererFactory;
import prefuse.render.LabelRenderer;
import prefuse.util.ColorLib;
import prefuse.visual.VisualItem;

import java.awt.*;

public class GraphView extends Display {

    public GraphView(Graph graph) {
        super(new Visualization());
        m_vis.addGraph("graph", graph);
        m_vis.setValue("graph.edges", null, VisualItem.INTERACTIVE, Boolean.FALSE);

        final LabelRenderer label = new LabelRenderer("id");
        label.setRoundedCorner(8, 8);
        m_vis.setRendererFactory(new DefaultRendererFactory(label));

        final ColorAction fill = new ColorAction("graph.nodes", VisualItem.FILLCOLOR, ColorLib.rgb(200, 200, 255));
        fill.add(VisualItem.FIXED, ColorLib.rgb(255, 100, 100));
        fill.add(VisualItem.HIGHLIGHT, ColorLib.rgb(255, 200, 125));

        final ActionList draw = new ActionList();
        draw.add(fill);
        draw.add(new ColorAction("graph.nodes", VisualItem.STROKECOLOR, 0));
        draw.add(new ColorAction("graph.nodes", VisualItem.TEXTCOLOR, ColorLib.rgb(0, 0, 0)));
        draw.add(new ColorAction("graph.edges", VisualItem.FILLCOLOR, ColorLib.gray(200)));
        draw.add(new ColorAction("graph.edges", VisualItem.STROKECOLOR, ColorLib.gray(200)));
        m_vis.putAction("draw", draw);

        final ActionList animate = new ActionList(Activity.INFINITY);
        animate.add(new ForceDirectedLayout("graph"));
        animate.add(fill);
        animate.add(new RepaintAction());
        m_vis.putAction("layout", animate);

        m_vis.runAfter("draw", "layout");

        this.setSize(1920, 1080);
        this.pan(350, 350);
        this.setForeground(Color.GRAY);
        this.setBackground(Color.WHITE);
        this.addControlListener(new FocusControl(1));
        this.addControlListener(new DragControl());
        this.addControlListener(new PanControl());
        this.addControlListener(new WheelZoomControl(true, false));
        this.addControlListener(new NeighborHighlightControl());

        m_vis.run("draw");
    }

}
