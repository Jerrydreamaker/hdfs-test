import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * Created by Dreamaker on 2017/11/23.
 */
public class ReadThread extends Thread {
    private FileSystem fs;
    private int threadOrder;
    private String readPath;
    private String threadReadPath;
    private long[] threadExecTime;
    private byte[] buffer;

    public ReadThread(FileSystem fs, int threadOrder, long bufferSize, String readPath, long[] threadExecTime) {
        this.fs = fs;
        this.threadOrder = threadOrder;
        this.threadExecTime = threadExecTime;
        /*
        如果readPath以“/”结尾，为readPath添加“/”。
        threadReadPath=readPath+thread-i
         */
        if (readPath.charAt(readPath.length() - 1) == 47) {
            this.readPath = readPath;
            this.threadReadPath = this.readPath + "thread-" + threadOrder;
        } else {
            this.readPath = readPath + "/";
            this.threadReadPath = this.readPath + "/thread-" + threadOrder;
        }
        this.buffer = new byte[(int) bufferSize];
    }

    public void run() {
        try {
            FileStatus[] subDir = fs.listStatus(new Path(threadReadPath + "/"));//返回所有子目录
            int readLen;
            long threadStartTime = System.currentTimeMillis();

            for (int i = 1; i <= subDir.length; i++) {
                FileStatus[] fileOfSubDir = fs.listStatus(new Path(threadReadPath + "/" + i));//返回所有文件

                for (int j = 0; j < fileOfSubDir.length; j++) {
                    FSDataInputStream fsDataInputStream = fs.open(fileOfSubDir[j].getPath());
                    readLen = 0;
                    readLen = fsDataInputStream.read(this.buffer);
                    while (readLen != -1) {
                        readLen = fsDataInputStream.read(buffer);
                    }
                    fsDataInputStream.close();


                }
                long threadEndTime = System.currentTimeMillis();
                threadExecTime[threadOrder] = threadEndTime - threadStartTime;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
