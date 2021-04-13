package com.github.vitalibo.grapes.processing.core.util;

import lombok.RequiredArgsConstructor;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

@RequiredArgsConstructor
public class GraphMLWriter implements AutoCloseable {

    private final XMLEventFactory eventFactory;
    private final OutputStream outputStream;
    private final XMLEventWriter writer;

    public GraphMLWriter(OutputStream outputStream) {
        this(XMLOutputFactory.newInstance(), XMLEventFactory.newInstance(), outputStream);
    }

    public GraphMLWriter(XMLOutputFactory writerFactory, XMLEventFactory eventFactory, OutputStream outputStream) {
        this(Throwing.function(writerFactory::createXMLEventWriter), eventFactory, outputStream);
    }

    public GraphMLWriter(Function<OutputStream, XMLEventWriter> writerFactory, XMLEventFactory eventFactory, OutputStream outputStream) {
        this(eventFactory, outputStream, writerFactory.apply(outputStream));
    }

    public void createStartDocument() throws XMLStreamException {
        writer.add(eventFactory.createStartDocument());

        writer.add(eventFactory.createCharacters(System.lineSeparator()));
        writer.add(eventFactory.createStartElement("", "", "graphml"));
        writer.add(eventFactory.createNamespace("", "http://graphml.graphdrawing.org/xmlns"));

        writer.add(eventFactory.createCharacters(System.lineSeparator() + "\t"));
        writer.add(eventFactory.createStartElement("", "", "graph"));
        writer.add(eventFactory.createAttribute("edgedefault", "undirected"));
    }

    public void createNodeKey(String id, String name, String type) throws XMLStreamException {
        createKey(id, "node", name, type);
    }

    public void createEdgeKey(String id, String name, String type) throws XMLStreamException {
        createKey(id, "edge", name, type);
    }

    private void createKey(String id, String element, String name, String type) throws XMLStreamException {
        writer.add(eventFactory.createCharacters(System.lineSeparator() + "\t\t"));
        writer.add(eventFactory.createStartElement("", "", "key"));
        writer.add(eventFactory.createAttribute("id", id));
        writer.add(eventFactory.createAttribute("for", element));
        writer.add(eventFactory.createAttribute("attr.name", name));
        writer.add(eventFactory.createAttribute("attr.type", type));
        writer.add(eventFactory.createEndElement("", "", "key"));
    }

    public void createNode(int node) throws XMLStreamException {
        createNode(node, Collections.emptyMap());
    }

    public void createNode(int node, Map<String, ?> attributes) throws XMLStreamException {
        writer.add(eventFactory.createCharacters(System.lineSeparator() + "\t\t"));
        writer.add(eventFactory.createStartElement("", "", "node"));
        writer.add(eventFactory.createAttribute("id", String.valueOf(node)));
        createAttributes(attributes);
        writer.add(eventFactory.createEndElement("", "", "node"));
    }

    public void createEdge(int source, int target) throws XMLStreamException {
        createEdge(source, target, Collections.emptyMap());
    }

    public void createEdge(int source, int target, Map<String, ?> attributes) throws XMLStreamException {
        writer.add(eventFactory.createCharacters(System.lineSeparator() + "\t\t"));
        writer.add(eventFactory.createStartElement("", "", "edge"));
        writer.add(eventFactory.createAttribute("source", String.valueOf(source)));
        writer.add(eventFactory.createAttribute("target", String.valueOf(target)));
        createAttributes(attributes);
        writer.add(eventFactory.createEndElement("", "", "edge"));
    }

    private void createAttributes(Map<String, ?> attributes) throws XMLStreamException {
        for (Map.Entry<String, ?> entry : attributes.entrySet()) {
            writer.add(eventFactory.createCharacters(System.lineSeparator() + "\t\t\t"));
            writer.add(eventFactory.createStartElement("", "", "data"));
            writer.add(eventFactory.createAttribute("key", entry.getKey()));
            writer.add(eventFactory.createCharacters(String.valueOf(entry.getValue())));
            writer.add(eventFactory.createEndElement("", "", "data"));
        }

        if (!attributes.isEmpty()) {
            writer.add(eventFactory.createCharacters(System.lineSeparator() + "\t\t"));
        }
    }

    public void createEndElement() throws XMLStreamException {
        writer.add(eventFactory.createCharacters(System.lineSeparator() + "\t"));
        writer.add(eventFactory.createEndElement("", "", "graph"));

        writer.add(eventFactory.createCharacters(System.lineSeparator()));
        writer.add(eventFactory.createEndElement("", "", "graphml"));
    }

    @Override
    public void close() throws XMLStreamException, IOException {
        writer.flush();
        writer.close();
        outputStream.flush();
        outputStream.close();
    }

}
