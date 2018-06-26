import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Vector;

/**
 * Created by Dreamaker on 2017/11/22.
 */
public class Client {
    private static String USAGE="hadoop jar hdfs_test.jar  -optype write -fileNum 100 -fileSize 1KB -bufferSize 1KB " +
            "-path /write -threadNum 10\n" +
            "hadoop jar hdfs_test.jar  -optype read   -fileNum 100 -fileSize 1KB -bufferSize 1KB -path /write " +
            "-threadNum 10\n";
    private static int threadNum;
    private static int fileNum;
    private static long fileSize;
    private static long bufferSize=0;
    private static byte[] buffer;
    private static String path;
    private static long execStartTime;
    private static long execEndTime;
    private static long execTime;
    private static long[] threadExecTime;

    private static enum OPTYPE {
        WRITE("write"),
        READ("read");
        private String type;
        private OPTYPE(String t) {
            this.type = t;
        }
    }
    private static OPTYPE optype;
    private static Vector<Thread> threadVector;
    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length == 0) {
            System.out.println(USAGE);
            return;
        } else {
            Configuration conf=new Configuration();
            conf.addResource("core-site.xml");
            FileSystem fs=FileSystem.get(conf);
            int ret = init(args);
            if (ret < 0) {
                System.exit(1);
            } else {
                execStartTime = System.currentTimeMillis();
                switch (optype.ordinal()) {
                    case (0): {
                        buffer=new byte[(int)bufferSize];
                        for(int i=0;i<buffer.length;i++){
                            buffer[i]=(byte)('0' + i % 50);
                        }
                        for (int i = 0; i < threadNum; i++) {
                            Thread writeThread = new WriteThread(fs, i, fileNum, fileSize, buffer, path, threadExecTime);
                            threadVector.add(writeThread);
                            writeThread.start();
                        }

                    }
                    break;
                    case (1): {
                        for (int i = 0; i < threadNum; i++) {
                            Thread readThread = new ReadThread(fs,i, bufferSize,path,threadExecTime);
                            threadVector.add(readThread);
                            readThread.start();

                        }

                    }
                }
            }
        }
        for (Thread thread:threadVector){
            thread.join();
        }
        execEndTime = System.currentTimeMillis();
        execTime = execEndTime - execStartTime;
        System.out.println("totalFileNum:" + (threadNum * fileNum));
        System.out.println("totalFileSize:" + (common.toMB(threadNum * fileNum * fileSize))+"MB");
        System.out.println("execTime:" + execTime+"ms");
        System.out.println("thread avg time:" + common.getAvgOfTime(threadExecTime, threadNum)+"ms");
        System.out.println("OPS:" + (threadNum * fileNum*1000/ execTime));
        System.out.println("MPS:" + (common.toMB(threadNum * fileNum * fileSize)*1000/execTime));

    }

    private static  int init(String[] args){

        for (int i=0;i<args.length;i++){
            if (args[i].equals("-optype")) {
                i++;
                if (args[i] .equals("write")) {
                    optype = OPTYPE.WRITE;
                } else if (args[i].equals("read")) {
                    optype = OPTYPE.READ;
                } else {
                    System.err.println("Error argument optype!");
                }
            }
            else if (args[i].equals("-fileNum")){
                i++;
                fileNum=Integer.valueOf(args[i]);
            }
            else if (args[i].equals("-threadNum")){
                i++;
                threadNum=Integer.valueOf(args[i]);
            }
            else if (args[i].equals("-fileSize")){
                i++;
                fileSize=common.parseSize(args[i]);
            }
            else if (args[i].equals("-bufferSize")){
                i++;
                bufferSize=common.parseSize(args[i]);
            }

            else if (args[i].equals("-path")){
                i++;
                path=args[i];
            }
            else {
                System.out.println("Error Args!");
                return -1;
            }
        }

        threadVector=new Vector<Thread>();//初始化threadVector
        threadExecTime=new long[threadNum];
        return 0;
    }
}
