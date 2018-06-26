/**
 * Created by Dreamaker on 2017/11/14.
 */
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class common {
    private static final long MEGA;

    public common() {
    }

    static float toMB(long bytes) {
        return (float)bytes / (float)MEGA;
    }

    static long parseSize(String arg) {
        String[] args = arg.split("\\D", 2);
        assert args.length <= 2;
        long nrBytes = Integer.parseInt(args[0]);
        String bytesMult = arg.substring(args[0].length());
        return nrBytes * ByteMultiple.parseString(bytesMult).value();
    }

    public static Map<String, String> analysizeData(long[] rawData) {
        HashMap resultMap = new HashMap();
        Arrays.sort(rawData);
        int index_50 = (int)((double)rawData.length * 0.5D);
        int index_60 = (int)((double)rawData.length * 0.6D);
        int index_70 = (int)((double)rawData.length * 0.7D);
        int index_80 = (int)((double)rawData.length * 0.8D);
        int index_90 = (int)((double)rawData.length * 0.9D);
        int index_95 = (int)((double)rawData.length * 0.95D);
        resultMap.put("Min", String.valueOf(rawData[0]));
        resultMap.put("Max", String.valueOf(rawData[rawData.length - 1]));
        resultMap.put("50", String.valueOf(rawData[index_50]));
        resultMap.put("60", String.valueOf(rawData[index_60]));
        resultMap.put("70", String.valueOf(rawData[index_70]));
        resultMap.put("80", String.valueOf(rawData[index_80]));
        resultMap.put("90", String.valueOf(rawData[index_90]));
        resultMap.put("95", String.valueOf(rawData[index_95]));
        return resultMap;
    }

    static {
        MEGA = ByteMultiple.MB.value();
    }

    static enum ByteMultiple {
        B(1L),
        KB(1024L),
        MB(1048576L),
        GB(1073741824L),
        TB(1099511627776L);

        private long multiplier;

        private ByteMultiple(long mult) {
            this.multiplier = mult;
        }

        long value() {
            return this.multiplier;
        }

        static ByteMultiple parseString(String sMultiple) {
            if(sMultiple != null && !sMultiple.isEmpty()) {
                String sMU = sMultiple.toUpperCase();
                if(B.name().toUpperCase().endsWith(sMU)) {
                    return B;
                } else if(KB.name().toUpperCase().endsWith(sMU)) {
                    return KB;
                } else if(MB.name().toUpperCase().endsWith(sMU)) {
                    return MB;
                } else if(GB.name().toUpperCase().endsWith(sMU)) {
                    return GB;
                } else if(TB.name().toUpperCase().endsWith(sMU)) {
                    return TB;
                } else {
                    throw new IllegalArgumentException("Unsupported ByteMultiple " + sMultiple);
                }
            } else {
                return MB;
            }
        }
    }

    public static long getAvgOfTime(long[] time,int num){
        long sum=0;
        for (int i=0;i<num;i++){
            sum+=time[i];
        }
        return sum/num;
    }
}

