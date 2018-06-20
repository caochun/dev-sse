package Demo;

import com.dataartisans.flink_demo.datatypes.TickRecord;
import com.dataartisans.flink_demo.datatypes.WasteRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.Date;
import java.util.Random;

public class SJ2Kafka {
    long count = 0;
    public static void main(String[] args){
        File file = new File("./data/Tick.csv");
        if(!file.exists()){
            System.out.println("file not exsits");
        }
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            SJ2Kafka w = new SJ2Kafka();
            w.generateOrderedStream(br);
        } catch (Exception e){
            System.out.println("fsdfsf");
            System.out.println(e.getMessage());
        }
    }
    private long maxDelayMsecs = 6 * 1000;
    private long watermarkDelayMSecs = 600;
    private long servingSpeed = 600000000;
    private void generateOrderedStream(BufferedReader br) {

        long servingStartTime = new Date().getTime();
        long dataStartTime = 0L;
        long nextWatermark = 0L;
        long nextWatermarkServingTime = 0L;
        String line;
        LocalProducer prod = new LocalProducer("host13:9092", "sj");
        try {
            if ((line = br.readLine()) != null) {
                TickRecord waste = TickRecord.fromString(line);

                dataStartTime = waste.time().getMillis();
                // initialize watermarks
                nextWatermark = dataStartTime + watermarkDelayMSecs;
                nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark);
                // emit first event
                prod.send(""+ count, line);
                count ++;
//                sourceContext.collectWithTimestamp(waste, waste.time.getMillis
            } else {
                return;
            }
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                TickRecord waste = TickRecord.fromString(line);
                long eventTime = waste.time().getMillis();
                long now = new Date().getTime();
                long eventServingTime = toServingTime(servingStartTime, dataStartTime, eventTime);

                // get time to wait until event and next watermark needs to be emitted
                long eventWait = eventServingTime - now;
                long watermarkWait = nextWatermarkServingTime - now;

                long sleeptime = 0;
                if (eventWait < watermarkWait) {
                    // wait to emit next event
                    if(eventTime > 0){
                        sleeptime = eventWait;
                    }
//                    sleeptime = 0;
                    Thread.sleep(sleeptime < 0? -sleeptime: sleeptime);
                } else if (eventWait > watermarkWait) {
                    // wait to emit watermark
                    if (watermarkWait > 0){
                        sleeptime = watermarkWait;
                    }
//                    sleeptime = 0;

                    Thread.sleep(sleeptime < 0? -sleeptime: sleeptime);
                    // emit watermark
//                    sourceContext.emitWatermark(new Watermark(nextWatermark))
                    // schedule next watermark
                    nextWatermark = nextWatermark + watermarkDelayMSecs;
                    nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark);
                    // wait to emit event
                    long remainWait = eventWait - watermarkWait;
                    sleeptime = 0;
                    if(remainWait > 0){
                        sleeptime = remainWait;
                    }
//                    sleeptime = 0;

                    Thread.sleep(sleeptime < 0? -sleeptime: sleeptime);
                } else if (eventWait == watermarkWait) {
                    // wait to emit watermark
                    if (watermarkWait > 0){
                        sleeptime = watermarkWait;
                    }
//                    sleeptime = 0;

                    Thread.sleep(sleeptime < 0? -sleeptime: sleeptime);
                    // emit watermark
//                    sourceContext.emitWatermark(new Watermark(nextWatermark - 1))
                    // schedule next watermark
                    nextWatermark = nextWatermark + watermarkDelayMSecs;
                    nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark);
                }
                // emit event
//                sourceContext.collectWithTimestamp(waste, waste.time.getMillis)
                prod.send("" + count, line);
                count++;
//                Thread.sleep(500);
            }
        } catch(Exception e){
            System.out.println(e.getMessage());
            System.out.println(e.toString());
            e.printStackTrace(new PrintStream(System.out));
        }
    }
    private long toServingTime(long servingStartTime, long dataStartTime, long eventTime){
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    private long getNormalDelayMsecs(Random rand){
        long delay = -1L;
        long x = maxDelayMsecs / 2;
        while (delay < 0 || delay > maxDelayMsecs) {
            delay = (long)rand.nextGaussian() * x + x;
        }
        return delay;
    }
}
