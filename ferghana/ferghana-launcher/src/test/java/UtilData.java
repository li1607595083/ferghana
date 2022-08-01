import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class UtilData {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat format01 = new SimpleDateFormat("yyyy-MM");
    public static Random rand=new Random();
    public static final String[] name = new String[]{"熊大","熊二","张三","李四","王五","赵六","麻七","八戒","九宫","十杀"};
    public static final String[] address = new String[]{"北京","上海","河北省","四川省","湖北省","甘肃省","山东省","江苏省","浙江省","海南省"};
    public int i = 0;

    public String randomNumber(int num){
        String number = "";
        for(int a=0;a<num;a++){
            number+=rand.nextInt(10);
        }
        return number;
    }

    public static String custName(){
        int a= (int) Math.floor(Math.random()*name.length);
        return name[a];
    }

    public String age(){
        return rand.nextInt(65)+"";
    }

    public String getRandomIp() {
        int[][] range = { { 607649792, 608174079 }, // 36.56.0.0-36.63.255.255
                { 1038614528, 1039007743 }, // 61.232.0.0-61.237.255.255
                { 1783627776, 1784676351 }, // 106.80.0.0-106.95.255.255
                { 2035023872, 2035154943 }, // 121.76.0.0-121.77.255.255
                { 2078801920, 2079064063 }, // 123.232.0.0-123.235.255.255
                { -1950089216, -1948778497 }, // 139.196.0.0-139.215.255.255
                { -1425539072, -1425014785 }, // 171.8.0.0-171.15.255.255
                { -1236271104, -1235419137 }, // 182.80.0.0-182.92.255.255
                { -770113536, -768606209 }, // 210.25.0.0-210.47.255.255
                { -569376768, -564133889 }, // 222.16.0.0-222.95.255.255
        };
        int index = rand.nextInt(10);
        String ip = num2ip(range[index][0] + new Random().nextInt(range[index][1] - range[index][0]));
        return ip;
    }

    /*
     * 将十进制转换成IP地址
     */
    public String num2ip(int ip) {
        int[] b = new int[4];
        String x = "";
        b[0] = (int) ((ip >> 24) & 0xff);
        b[1] = (int) ((ip >> 16) & 0xff);
        b[2] = (int) ((ip >> 8) & 0xff);
        b[3] = (int) (ip & 0xff);
        x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);
        return x;
    }

    public String timestamp(){
        return sdf.format(randomDate(null,null));
    }

    public String timestamp(Date date){
        return sdf.format(date);
    }

    public String address(){
        int a = (int) Math.floor(Math.random()*address.length);
        return address[a]+i;
    }

    public Date randomDate(String beginDate,String endDate){
        if(beginDate == null){
            beginDate = "2019-08-05 00:00:00";
        }
        if(endDate == null) {
            endDate = "2019-08-06 00:00:00";
        }
        try {
            Date start = sdf.parse(beginDate);
            Date end = sdf.parse(endDate);
            long date = random(start.getTime(),end.getTime());
            return new Date(date);
        }catch (Exception e){
            e.printStackTrace();
        }
        return new Date();
    }

    public long random(long begin,long end){
        long rtn = begin + (long)(Math.random() * (end - begin));
        return rtn;
    }

    public static int getNum(int start,int end) {
        return (int)(Math.random()*(end-start+1)+start);
    }

    /**
     * 返回手机号码
     */
    public static String[] telFirst="134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",");
    public static String getTel() {
        int index=getNum(0,telFirst.length-1);
        String first=telFirst[index];
        String second=String.valueOf(getNum(1,888)+10000).substring(1);
        String third=String.valueOf(getNum(1,9100)+10000).substring(1);
        return first+second+third;
    }

    /**
     * 返回电子账号
     */
    public static String getAcctNo(){
        String acctNoleftPart = "62284803958087";
        String acctNoRightPart = String.valueOf(getNum(1,91000)+100000).substring(1);
        return acctNoleftPart + acctNoRightPart;
    }

    /**
     * 返回客户编号
     */
    public static String getCustNo(){
        String acctNoleftPart = "101520";
        String acctNoRightPart = String.valueOf(getNum(1,9100)+10000).substring(1);
        return acctNoleftPart + acctNoRightPart;
    }

    public static String getTranNo(){
        String uuid = UUID.randomUUID().toString().replaceAll("-","");
        return uuid;
    }



    public static void main(String[] args) {
        System.out.println(getTranNo());
    }

}
