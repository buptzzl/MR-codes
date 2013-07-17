package com.emar.recsys.user.zonerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

import com.emar.util.exp.UrlPair;
import com.sun.org.apache.xpath.internal.operations.Bool;

/**
 * 定义 用于在 MR 的 Reduce中先到达的KEY的Writable 对象
 * @author zhoulm
 *
 */
public class PriorPair implements WritableComparable<PriorPair> {
	private Text key;
	private BooleanWritable flag;
	
	private void setKV(Text k, BooleanWritable f) {
		this.key = k;
		this.flag = f;
	}
	
	public PriorPair() {
		this.setKV(new Text(), new BooleanWritable(false));
	}
	public PriorPair(Text k, BooleanWritable f) {
		this.setKV(k, f);
	}
	public PriorPair(Text t) {
		this.setKV(t, new BooleanWritable(false));
	}
	
	public Text getFirst() {
		return this.key;
	}
	public void setFirst(Text t) {
		this.key = t;
	}
	public void setFirst(String s) {
		this.key.set(s);
	}
	
	public BooleanWritable getFlag() {
		return this.flag;
	}
	public void setFlag(BooleanWritable f) {
		this.flag = f;
	}
	public void setFlag(boolean f) {
		this.flag.set(f);
	}
	
	public String toString() {
		return this.key.toString() + "\u0001" + this.flag.toString(); 
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key.readFields(in);
		in.skipBytes(1);
		this.flag.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out);
		out.writeByte(0x01);
		this.flag.write(out);
	}

	@Override
	public int compareTo(PriorPair obj) {  
		// GroupingComparator SortComparator 默认调用， 默认为 this-that, 排序为ASC
//		return this.key.compareTo(obj.key);
//		/*
		if(this.flag.get()) {
			if(obj.flag.get()) {  // 两者都为高优先级
				return this.key.compareTo(obj.key);
			} else {
				return 1; 
			}
		} else if(obj.getFlag().get() == false) {  // 两者都为低优先级
			return this.key.compareTo(obj.key);
		}
		return 1;
//		*/
	}
	
	@Override
	public int hashCode() {  // HashPartitioner 调用
		return this.key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof PriorPair)) {
			return false;
		}
		if(obj == this) {
			return true;
		}
		return this.key.equals(((PriorPair)obj).getFirst());
	}
}
