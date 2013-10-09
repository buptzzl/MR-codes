package com.emar.recsys.user.util.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

/**
 * 定义 用于在 MR 的 Reduce中先到达的KEY的Writable 对象， 拓展PriorPair的优先级表示为int
 * 
 * @author zhoulm
 * 
 */
public class PairTInt implements WritableComparable<PairTInt> {
	private Text key;
	private VIntWritable flag;

	private void setKV(Text k, VIntWritable f) {
		this.key = k;
		this.flag = f;
	}

	public PairTInt() {
		this.setKV(new Text(), new VIntWritable(0));
	}

	public PairTInt(Text k, VIntWritable f) {
		this.setKV(k, f);
	}

	public PairTInt(Text t) {
		this.setKV(t, new VIntWritable(0));
	}

	public PairTInt(String s) {
		this.setKV(new Text(s), new VIntWritable(0));
	}

	public PairTInt(String s, int b) {
		this.setKV(new Text(s), new VIntWritable(b));
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

	public VIntWritable getFlag() {
		return this.flag;
	}

	public void setFlag(VIntWritable f) {
		this.flag = f;
	}

	public void setFlag(int f) {
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
	public int hashCode() { // HashPartitioner 调用
		return this.key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof PairTInt)) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		return this.key.equals(((PairTInt) obj).getFirst());
	}

	@Override
	public int compareTo(PairTInt obj) {
		int c = this.key.compareTo(obj.key);
		if (c == 0) {
			c = this.flag.compareTo(obj.flag);
		}
		return c;
	}
}
