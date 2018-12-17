package com.revature.util;

import java.io.*;
import java.util.Set;

import org.apache.hadoop.io.Text;

import java.lang.Integer;


public class MyMapWritable implements WritableComparable<MapWritable>{
	private Text first;
	private Text second;
	
	public MyMapWritable() {
		set(new Text(), new Text());
	}
	
	public MyMapWritable(String first, String second) {
		set(new Text(first), new Text(second));
	}
	
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	
	public String getFirstString() {
		return new String(first.toString());
	}
	
	public String getSecondString() {
		return new String(second.toString());
	}
	public Text getSecond() {
		return second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
	first.write(out);
	second.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	first.readFields(in);
	second.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof MyMapWritable) {
			MyMapWritable tp = (MyMapWritable) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}
	
	@Override
	public int compareTo(MyMapWritable tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}

}