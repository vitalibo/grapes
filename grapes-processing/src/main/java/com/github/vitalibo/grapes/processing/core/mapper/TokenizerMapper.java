package com.github.vitalibo.grapes.processing.core.mapper;

import com.github.vitalibo.grapes.processing.core.util.Throwing;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

@RequiredArgsConstructor
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    private final Text word;
    private final Set<String> patternsToSkip;
    private boolean caseSensitive;

    public TokenizerMapper() {
        this(new Text(), new HashSet<>());
    }

    @Override
    public void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();
        caseSensitive = configuration.getBoolean("grapes.case.sensitive", true);

        if (configuration.getBoolean("grapes.skip.patterns", false)) {
            Arrays.stream(context.getCacheFiles())
                .map(patternsURI -> new Path(patternsURI.getPath()))
                .map(Path::getName)
                .flatMap(Throwing.function(name -> new BufferedReader(new FileReader(name))
                    .lines()))
                .forEach(patternsToSkip::add);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = caseSensitive ? value.toString() : value.toString().toLowerCase();
        for (String pattern : patternsToSkip) {
            line = line.replaceAll(pattern, "");
        }

        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, ONE);

            Counter counter = context.getCounter("Custom Counters", "Input words");
            counter.increment(1);
        }
    }

}
