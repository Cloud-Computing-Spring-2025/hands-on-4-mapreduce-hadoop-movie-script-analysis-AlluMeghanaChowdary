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
        // Split the line by ':', character name and dialogue
        String line = value.toString();
        String[] parts = line.split(":");
        if (parts.length < 2) {
            return; // Skip invalid lines
        }

        String character = parts[0].trim();  // Character's name
        String dialogue = parts[1].trim();   // Dialogue

        // Tokenize the dialogue and count words
        StringTokenizer tokenizer = new StringTokenizer(dialogue);
        while (tokenizer.hasMoreTokens()) {
            String wordText = tokenizer.nextToken().toLowerCase().replaceAll("[^a-z]", "");
            if (!wordText.isEmpty()) {
                word.set(wordText);
                characterWord.set(character + ":" + wordText);
                context.write(characterWord, one);  // Emit the word count for each character
            }
        }
    }
}
