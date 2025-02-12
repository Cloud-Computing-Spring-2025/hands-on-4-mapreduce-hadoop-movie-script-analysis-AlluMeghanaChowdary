package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(":");
        if (parts.length < 2) {
            return; // Skip invalid lines
        }

        String characterName = parts[0].trim();
        String dialogue = parts[1].trim();

        HashSet<String> uniqueWords = new HashSet<>();
        StringTokenizer tokenizer = new StringTokenizer(dialogue);
        while (tokenizer.hasMoreTokens()) {
            String wordText = tokenizer.nextToken().toLowerCase().replaceAll("[^a-z]", "");
            if (!wordText.isEmpty()) {
                uniqueWords.add(wordText);  // Collect unique words
            }
        }

        // Emit the character and the unique words they have used
        for (String uniqueWord : uniqueWords) {
            word.set(uniqueWord);
            context.write(new Text(characterName), word);  // Emit character and unique word
        }
    }
}
