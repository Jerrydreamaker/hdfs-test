import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Dreamaker on 2017/11/22.
 */
public class WriteThread  extends Thread{
    private FileSystem fs;
    private int threadOrder;
    private int fileNum;
    private long fileSize;
    private byte[] buffer;
    private String writePath;
    private String threadWritePath;
    private long[] threadExecTime;

    public WriteThread(FileSystem fs, int threadOrder, int fileNum, long fileSize, byte[] buffer, String writePath,
                       long[] threadExecTime) {
        this.fs = fs;
        this.threadOrder = threadOrder;
        this.fileNum = fileNum;
        this.fileSize = fileSize;
        this.buffer = buffer;
        this.threadExecTime = threadExecTime;
        /*
        如果readPath以“/”结尾，为readPath添加“/”。
        threadReadPath=readPath+thread-i
         */
        if(writePath.charAt(writePath.length() - 1) == 47) {
            this.writePath = writePath;
            this.threadWritePath = writePath + "thread-" + threadOrder;
        }
        else {
            this.writePath = writePath + "/";
            this.threadWritePath = writePath + "/thread-" + threadOrder;
        }
    }
    public void run(){

        int sdnOrder=0;
        for (int i=0;i<fileNum;i++){
            FSDataOutputStream fsDataOutputStream=null;
            String subPath = null;
            int maxFileNum = 1000;
            if (i % maxFileNum == 0) {
                sdnOrder++;
            }
            try {
                fsDataOutputStream=fs.create(new Path(threadWritePath+"/"+sdnOrder+"/"+(i%maxFileNum)));
                long threadStartTime=System.currentTimeMillis();
                int writeCounter = 0;
                while (writeCounter < fileSize / buffer.length) {
                    fsDataOutputStream.write(buffer);
                    writeCounter++;
                }
                long threadEndTime=System.currentTimeMillis();
                threadExecTime[threadOrder]=threadEndTime-threadStartTime;
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                try {
                    fsDataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
