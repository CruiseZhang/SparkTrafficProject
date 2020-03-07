package com.bjsxt.spark.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 字符串工具类
 * @author Administrator
 *
 */
public class StringUtils {

    /**
     * 判断字符串是否为空
     * @param str 字符串
     * @return 是否为空
     */
    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    /**
     * 判断字符串是否不为空
     * @param str 字符串
     * @return 是否不为空
     */
    public static boolean isNotEmpty(String str) {
        return str != null && !"".equals(str);
    }

    /**
     * 截断字符串两侧的逗号
     * @param str 字符串
     * @return 字符串
     */
    public static String trimComma(String str) {
        if(str.startsWith(",")) {
            str = str.substring(1);
        }
        if(str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     * @param str
     * @return
     */
    public static String fulfuill(String str) {
    	  if(str.length() == 1) 
            return "0" + str;
    	  return str;
    }
    
    
    /**
     * 补全num位数字
     * 将给定的字符串前面补0，使字符串的长度为num位。
     * 
     * @param str
     * @return
     * 举例，num是传入的5，str传"102"
     */
    public static String fulfuill(int num,String str) {
        if(str.length() == num) {
            return str;
        } else {
        	int fulNum = num-str.length();     // 此时5-3就是2
        	String tmpStr  =  "";
        	for(int i = 0; i < fulNum ; i++){   // 所以i<2循环两次
        		tmpStr += "0";                  // 则两个0
        	}
            return tmpStr + str;                // 所以"00102"，最后还是会变成5位数的字符串
        }
    }

    

    /**
     * 从拼接的字符串中提取k-v中的v
     * @param str 字符串
     * @param delimiter 分隔符
     * @param field 想要的key对应的value
     * @return 想要的key
     * 形如字符串name=zhangsan|age=18，返回18
     */
    public static String getFieldFromConcatString(String str,String delimiter, String field) {
        try {
            String[] fields = str.split(delimiter); // name=zhangsan|age=18分成两部分，name=zhangsan和age=18
            for(String concatField : fields) {
                // searchKeywords=|clickCategoryIds=1,2,3
                if(concatField.split("=").length == 2) {//等于2是因为数据可能如上一行例子=左右两边不是两部分
                    String fieldName = concatField.split("=")[0];   // key
                    String fieldValue = concatField.split("=")[1];  // value
                    if(fieldName.equals(field)) { // 如果传进来的key和待分割的key相等
                        return fieldValue;   // 返回value
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public static void main(String[] args) {
//    	System.out.println(getFieldFromConcatString("name=zhangsan|age=18","\\|","age"));
//      System.out.println(getFieldFromConcatString("name=|age=","\\|","age"));
//    	System.out.println(setFieldInConcatString("name=zhangsan|age=12","\\|","name","lisi"));
//    	Map<String, String> keyValuesFromConcatString = getKeyValuesFromConcatString("name=lisi","\\|");
//    	Set<Entry<String, String>> entrySet = keyValuesFromConcatString.entrySet();
//    	for(Entry<String, String> entry : entrySet) {
//    		System.out.println("key = "+entry.getKey()+",value = "+entry.getValue());
//    	}
    }

    /**
     * 从拼接的字符串中给符合要求的key设置新的value值
     * @param str 字符串
     * @param delimiter 分隔符
     * @param field 字段名
     * @param newFieldValue 新的field值
     * @return 字段值
     *  name=zhangsan|age=12
     */
    public static String setFieldInConcatString(String str,
                                                String delimiter, 
                                                String field, 
                                                String newFieldValue) {
       
        // searchKeywords=iphone7|clickCategoryIds=1,2,3
    	String[] fields = str.split(delimiter); //{"searchKeywords=iphone7","clickCategoryIds=1,2,3"}

        for(int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].split("=")[0];//i为0，得"searchKeywords=iphone7"，分割后取[0]即"searchKeywords"
            if(fieldName.equals(field)) { // 如果分割后的value与传进来value相等
                String concatField = fieldName + "=" + newFieldValue;
                fields[i] = concatField;
                break;
            }
        }

        // https://blog.csdn.net/qq_37856300/article/details/84340288
        StringBuffer buffer = new StringBuffer("");
        for(int i = 0; i < fields.length; i++) {
            buffer.append(fields[i]);
            if(i < fields.length - 1) {
                buffer.append("|");
            }
        }
        // 将StringBuffer，StringBuilder对象转换为String字符串，常用在需要输出的时候，因为StringBuffer和StringBuilder的对象不能直接输出，
        return buffer.toString();
    }

    /**
     * 给定字符串和分隔符，返回一个K,V格式的map
     *  name=zhangsan|age=18
     *  
     * @param str
     * @param delimiter
     * @return map<String,String>
     * 
     */
	public static Map<String, String> getKeyValuesFromConcatString(String str,String delimiter) {
		Map<String, String> map = new HashMap<>();
		try {
            String[] fields = str.split(delimiter);
            for(String concatField : fields) {
                // searchKeywords=|clickCategoryIds=1,2,3
                if(concatField.split("=").length == 2) {
                    String fieldName = concatField.split("=")[0];
                    String fieldValue = concatField.split("=")[1];
                    map.put(fieldName, fieldValue);
                }
            }
            return map;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
	}
	
	/**
	 * String 字符串转Integer数字
	 * @param str
	 * @return
	 */
	public static Integer convertStringtoInt(String str) {
		try {
			return Integer.parseInt(str);
		} catch (Exception e) {
			 e.printStackTrace();
		}
		return null;
		
	}

}
