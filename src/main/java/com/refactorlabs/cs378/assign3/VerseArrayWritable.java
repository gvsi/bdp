/**
 * Created by gvsi on 9/15/16.
 */

package com.refactorlabs.cs378.assign3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class VerseArrayWritable implements Writable {

    private String[] verses;

    public String[] getVerses() {
        return verses;
    }

    // Default constructor is required by Hadoop
    public VerseArrayWritable(){}

    public VerseArrayWritable(String[] verses) {
        this.verses = verses;
    }

    public void readFields(DataInput in) throws IOException {
        // Read the data out in the order it is written

        verses = new String[in.readInt()];
        for (int i = 0; i < verses.length; i++) {
            verses[i] = in.readUTF();
        }
    }

    public void write(DataOutput out) throws IOException {
        // Write the data out in the order it is read
        out.writeInt(verses.length);
        for (String s : verses) {
            out.writeUTF(s);
        }
    }

    // Sorts the array with a custom comparator object
    public void sort() {
        Arrays.sort(verses, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                String parts1[] = o1.split(":");
                String parts2[] = o2.split(":");

                String book1 = parts1[0];
                String book2 = parts2[0];
                int chapter1 = Integer.parseInt(parts1[1]);
                int chapter2 = Integer.parseInt(parts2[1]);
                int verse1 = Integer.parseInt(parts1[2]);
                int verse2 = Integer.parseInt(parts2[2]);

                if (book1.compareTo(book2) > 0) {
                    return 1;
                } else if (book1.compareTo(book2) < 0) {
                    return -1;
                }

                if (chapter1 > chapter2) {
                    return 1;
                } else if (chapter1 < chapter2) {
                    return -1;
                }

                if (verse1 > verse2) {
                    return 1;
                } else if (verse1 < verse2) {
                    return -1;
                }

                return 0;
            }
        });
    }

    public String toString() {
        return String.join(",", verses);
    }

}
