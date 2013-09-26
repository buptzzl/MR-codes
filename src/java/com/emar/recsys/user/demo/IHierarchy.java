package com.emar.recsys.user.demo;

/**
 * 对给定区间的数据 离散化（分层）， 按 整形（INT） 连续取值  索引作ID
 * @author zhoulm
 *
 */
public interface IHierarchy {
	
	/**  更新数据对象到 层次体系. 插入新的原始成功则返回True， 否则返回False  */
	public boolean update(Object obj);
	
	/** 删除插入的元素  */
	public boolean delete(Object obj);
	
	/**  设置 最大层次数  */
	public boolean setMaxLayer(Object obj);
	/** 返回最大层次数 */
	public int getMaxLayer();
	
	/**  返回给定对象 在层次中的ID. 有效区间为[1+,] 返回 -1 为无效层次, 0为层次数据为0(无效值)的返回。  */
	public int getLayer(Object obj);
	
	/** 返回原始层次数据规模  */
	public int getDataSize();
}
