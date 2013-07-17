/**    
 * @fun code store.
 *     
 */
package com.emar.util.exp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UrlPair implements WritableComparable<UrlPair> {
	private Text urlStr;
	private IntWritable urlNum;

	public UrlPair() {
		this.setValue(new Text(), new IntWritable(1));
	}

	public UrlPair(String urlStr, int urlNum) {
		this.setValue(new Text(urlStr), new IntWritable(urlNum));
	}

	public UrlPair(Text urlStr, IntWritable urlNum) {
		this.setValue(urlStr, urlNum);
	}

	public void setValue(Text urlStr, IntWritable urlNum) {
		this.urlStr = urlStr;
		this.urlNum = urlNum;
	}

	public Text getUrlStr() {
		return urlStr;
	}

	public IntWritable getUrlNum() {
		return urlNum;
	}

	public void setUrlStr(Text urlStr) {
		this.urlStr = urlStr;
	}

	public void setUrlNum(IntWritable urlNum) {
		this.urlNum = urlNum;
	}

	@Override
	public String toString() {
		return this.urlStr.toString() + "\\a" + this.urlNum.get();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.urlStr.readFields(in);
		in.skipBytes(1);
		this.urlNum.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.urlStr.write(out);
		out.writeByte(0x01);
		this.urlNum.write(out);
	}

	@Override
	public int compareTo(UrlPair o) {
		// TODO Auto-generated method stub
		// int cmp = this.urlStr.compareTo(o.urlStr);
		// if( cmp != 0 )
		// {
		// return cmp;
		// }
		// cmp = this.getUrlNum().compareTo(o.getUrlNum());
		// return cmp;
		int cmp = this.getUrlNum().compareTo(o.getUrlNum());
		return cmp;

	}

}
