package com.bjsxt.spark.skynet;

import com.bjsxt.spark.constant.Constants;
import com.bjsxt.spark.util.StringUtils;
import org.apache.spark.util.AccumulatorV2;

/**父类为AccumulatorV2<String, String>
 * isZero: 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。
 * copy: 拷贝一个新的AccumulatorV2
 * reset: 重置AccumulatorV2中的数据
 * add: 操作数据累加方法实现
 * merge: 合并数据
 * value: AccumulatorV2对外访问的数据结果
 */
public class SelfDefineAccumulator extends AccumulatorV2<String,String> {//两个string，前者是传进去的值，后者是返回的值
    String returnResult = "";

    /**
     * 与reset()方法保持一致，返回true。
     * @return
     */
    @Override
    public boolean isZero() {
        //normalMonitorCount=0|normalCameraCount=0|abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos=
        return "normalMonitorCount=0|normalCameraCount=0|abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos= ".equals(returnResult);
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SelfDefineAccumulator acc  = new SelfDefineAccumulator();
        acc.returnResult = this.returnResult;
        return acc;
    }

    /**
     * 每个分区初始值
     */
    @Override
    public void reset() {
        //normalMonitorCount=0|normalCameraCount=0|abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos=
        returnResult = Constants.FIELD_NORMAL_MONITOR_COUNT+"=0|"
                + Constants.FIELD_NORMAL_CAMERA_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_MONITOR_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_CAMERA_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"= ";
    }

    /**
     * 每个分区会拿着reset初始化的值 ，在各自的分区内相加
     * @param v
     */
    @Override
    public void add(String v) {
//        System.out.println("add returnResult ="+returnResult+", v="+v);
        returnResult = myAdd(returnResult,v);
    }

    /**
     * 每个分区最终的结果和初始值 returnResult=""  做累加
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        //这里初始值就是"" ,每个分区之后都是一个大的字符串

        SelfDefineAccumulator accumulator = (SelfDefineAccumulator)other;
//        System.out.println("merge   returnResult="+returnResult+" , accumulator.returnResult="+accumulator.returnResult);
        returnResult = myAdd(returnResult,accumulator.returnResult);
    }

    @Override
    public String value() {
        return returnResult;
    }

    /**v1是初始值也是最终结果值，v2是每次传进去要加给v1的值
     * v1：normalMonitorCount=0|normalCameraCount=0|abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos= 这里有个空格
     * v2：abnormalMonitorCount=1|abnormalCameraCount=3|abnormalMonitorCameraInfos="0002":07553,07554,07556
     * 返回v1：normalMonitorCount=1|normalCameraCount=3|abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos= ~0002":07553,07554,07556
     * */
    private String myAdd(String v1, String v2) {
//        System.out.println("myAdd v1="+v1);
//        System.out.println("myAdd v2="+v2);
        if(StringUtils.isEmpty(v1)){ // 自定义的类和方法，判断v1是否为空
            return v2;
        }
        //比如上例：abnormalMonitorCount=1，abnormalCameraCount=3，abnormalMonitorCameraInfos="0002":07553,07554,07556
        String[] valArr = v2.split("\\|");
        for (String string : valArr) { //abnormalMonitorCount=1
            String[] fieldAndValArr = string.split("=");//abnormalMonitorCount和1
            String field = fieldAndValArr[0];//abnormalMonitorCount
            String value = fieldAndValArr[1];//1
            String oldVal = StringUtils.getFieldFromConcatString(v1, "\\|", field);//0
            if(oldVal != null){
                //只有abnormalMonitorCameraInfos对应的value是string，所以单独拿出来if一下
                if(Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS.equals(field)){ //abnormalMonitorCameraInfos
                    //abnormalMonitorCameraInfos="0002":07553,07554,07556~"0005":07553,07554,07556~"0007":07553,07554,07556
                    if(value.startsWith(" ~")) {
                        value = value.substring(2);//舍去的前2个是1个空格和1个波浪线
                    }
                    v1 = StringUtils.setFieldInConcatString(v1, "\\|", field, oldVal + "~" + value);
                }else{
                    //abnormalMonitorCameraInfos以外的k都是int类型，直接加减就可以
                    int newVal = Integer.parseInt(oldVal)+Integer.parseInt(value); // 0+1
                    v1 = StringUtils.setFieldInConcatString(v1, "\\|", field, String.valueOf(newVal));//0变为1
                }
            }
        }
        return v1;
    }
}
