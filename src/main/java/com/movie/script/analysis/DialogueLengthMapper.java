package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(":");

        if (parts.length == 2) {
            character.set(parts[0].trim());
            String dialogue = parts[1].trim();
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            wordCount.set(tokenizer.countTokens());
            context.write(character, wordCount);
        }
    }
}
