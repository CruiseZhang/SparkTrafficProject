package com.bjsxt.spark.util;

import java.math.BigDecimal;

/**
 * 浮点数四舍五入工具类
 * @author Administrator
 *
 */
public class NumberUtils {

	/**
	 * 格式化小数
	 * @param num 原数字
	 * @param scale 四舍五入的位数
	 * @return 格式化小数
	 */
	public static double formatDouble(double num, int scale) {
		//https://www.cnblogs.com/zhangyinhua/p/11545305.html
		BigDecimal bd = new BigDecimal(num);  //当double（16位有效）都不满足的时候用BigDecimal
		return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue(); // 将BigDecimal对象中的值转换成双精度数
	}
	
}
