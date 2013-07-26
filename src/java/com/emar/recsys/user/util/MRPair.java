	package com.emar.recsys.user.util;

	import java.io.DataInput;
	import java.io.DataOutput;
	import java.io.IOException;

	import org.apache.hadoop.io.*;

	/**
	 * TODO 必须定义无参数的默认构造函数
	 * @author zhoulm
	 *
	 */
	public class MRPair<T extends Comparable&Writable, V extends Comparable&Writable> implements WritableComparable<MRPair> {
		private T key;
		private V prior;
		
		private void setKV(T k, V f) {
			this.key = k;
			this.prior = f;
		}
		
//		public MRPair() {
//			this.setKV(new T(), new V(false));
//		}
		public MRPair(T k, V f) {
			this.setKV(k, f);
		}
		public MRPair() { 
			key = null;
			prior = null;
		}
		
		public T getFirst() {
			return this.key;
		}
		public void setFirst(T t) {
			this.key = t;
		}
		
		public V getSecond() {
			return this.prior;
		}
		public void setSecond(V f) {
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
		public int compareTo(MRPair obj) {  
			int c = this.key.compareTo(obj.key);
			if(c == 0) {
				c = this.prior.compareTo(obj.prior);
			}
			return c;
		}
		
		@Override
		public int hashCode() {  // HashPartitioner 调用
			return this.key.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if(obj == null || !(obj instanceof MRPair)) {
				return false;
			}
			if(obj == this) {
				return true;
			}
			return this.key.equals(((MRPair)obj).getFirst());
		}
	}
