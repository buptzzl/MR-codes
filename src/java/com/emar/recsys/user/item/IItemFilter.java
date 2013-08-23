package com.emar.recsys.user.item;

import java.util.List;

/**
 * Item feature的通用过滤接口。
 * @author zhoulm
 *
 */

public interface IItemFilter {

	/** Item 使用前的预处理 */
	public abstract String prefilter(String line) ;
	
	/** Item 切分后的处理 */
	public abstract List<String> postFilter(List<String> atom); 
		
	/** 单个词项的过滤 */
	public abstract String sfilter(String ai) ;


}
