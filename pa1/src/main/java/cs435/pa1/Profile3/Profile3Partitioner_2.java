package cs435.pa1.Profile3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Profile3Partitioner_2 extends Partitioner<IntWritable, Text> {

    @Override
    public int getPartition(IntWritable key, Text Value, int numParitions){

        int frequency = key.get();

        switch(frequency) {
            case 1: return 29;
            case 2: return 28;
            case 3: return 27;
            case 4: return 26;
            case 5: return 25;
            case 6: return 24;
            case 7: return 23;
            case 8: return 22;
            case 9: return 21;
            case 10: return 20;
        }

        if (frequency > 10 && frequency <= 20)
            return 20;

        if (frequency > 20 && frequency <= 40)
            return 19;

        if (frequency > 40 && frequency <= 75)
            return 18;

        if (frequency > 75 && frequency <= 100)
            return 17;

        if (frequency > 100 && frequency <= 200)
            return 16;

        if (frequency > 200 && frequency <= 300)
            return 15;

        if (frequency > 300 && frequency <= 400)
            return 14;

        if (frequency > 400 && frequency <= 500)
            return 13;

        if (frequency > 500 && frequency <= 600)
            return 12;

        if (frequency > 600 && frequency <= 700)
            return 11;

        if (frequency > 700 && frequency <= 800)
            return 10;

        if (frequency > 800 && frequency <= 900)
            return 9;

        if (frequency > 900 && frequency <= 1000)
            return 8;

        if (frequency > 1000 && frequency <= 1200)
            return 7;

        if (frequency > 1200 && frequency <= 1400)
            return 6;

        if (frequency > 1400 && frequency <= 1600)
            return 5;

        if (frequency > 1600 && frequency <= 1800)
            return 4;

        if (frequency > 1800 && frequency <= 2000)
            return 3;

        if (frequency > 2000 && frequency <= 2200)
            return 2;

        if (frequency > 2200 && frequency <= 2400)
            return 1;

        return 0;

    }
}