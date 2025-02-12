package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(":");
        if (parts.length < 2) {
            return; // Skip invalid lines
        }

        String characterName = parts[0].trim();
        String dialogue = parts[1].trim();

        int length = dialogue.length(); // Calculate the total length of the dialogue
        wordCount.set(length);
        character.set(characterName);
        context.write(character, wordCount);  // Emit the dialogue length for each character
    }
}
