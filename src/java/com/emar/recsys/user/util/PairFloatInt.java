package com.emar.recsys.user.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

/**
 * 
 * 自定义Writable对象，必须定义无参数的默认构造函数；不能用泛型。
 * 
 * @author zhoulm key=FloatWritable, val=IntWritable
 * 
 */
public class PairFloatInt implements WritableComparable<PairFloatInt> {
	private FloatWritable key;
	private VIntWritable prior;

	private void setKV(FloatWritable k, VIntWritable f) {
		this.key = k;
		this.prior = f;
	}

	public PairFloatInt(FloatWritable k, VIntWritable f) {
		this.setKV(k, f);
	}

	public PairFloatInt() {
		key = new FloatWritable();
		prior = new VIntWritable();
	}

	public FloatWritable getFirst() {
		return this.key;
	}

	public void setFirst(FloatWritable t) {
		this.key = t;
	}

	public VIntWritable getSecond() {
		return this.prior;
	}

	public void setSecond(VIntWritable f) {
		this.prior = f;
	}

	public String toString() {
		return this.key.toString() + "\u0001" + this.prior.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key.readFields(in);
		in.skipBytes(1);
		this.prior.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out);
		out.writeByte(0x01);
		this.prior.write(out);
	}

	@Override
	public int compareTo(PairFloatInt obj) {
		int c = this.key.compareTo(obj.key);
		if (c == 0) {
			c = this.prior.compareTo(obj.prior);
		}
		return c;
	}

	@Override
	public int hashCode() { // HashPartitioner 调用
		return this.key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof PairFloatInt)) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		return this.key.equals(((PairFloatInt) obj).getFirst());
	}
}
