/**
 * @description: SqlFormatting
 * @author: ......
 * @create: 2022/3/314:18
 */
public class SqlFormatting {


    // SELECT *
    // FROM
    // 
    public static void main(String[] args) {

        int count = 0;
        double init001 = 22;
        double init002 = 17;

        while (init001 > init002){
            count++;
            init001 *= init001 * 1.06;
            init002 *= init002 * 1.03;
        }
        System.out.println(count);
    }

}
