package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        // Assuming lines are in the format "CHARACTER_NAME: dialogue here"
        if (line.contains(":")) {
            String[] parts = line.split(":", 2);
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Tokenize the dialogue to get words
            StringTokenizer itr = new StringTokenizer(dialogue);
            while (itr.hasMoreTokens()) {
                String wordInDialogue = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!wordInDialogue.isEmpty()) {
                    // Emit a combination of character name and word
                    characterWord.set(characterName + ":" + wordInDialogue);
                    context.write(characterWord, one);
                }
            }

            // Emit character name with the total number of words spoken in this line for dialogue length analysis
            context.write(new Text(characterName), new IntWritable(itr.countTokens()));
        }
    }
    
}
