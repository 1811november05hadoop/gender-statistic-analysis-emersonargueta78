package com.revature.util;

import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Integer;


public class TextPair implements WritableComparable<MapWritable>{
	private Text first;
	private Text second;
	
	public TextPair() {
		set(new Text(), new Text());
	}
	
	public TextPair(String first, String second) {
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
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}
	
	@Override
	public int compareTo(MapWritable tp) {
        Text firstKey = (Text)tp.keySet().toArray()[0];
		int cmp = first.compareTo(firstKey);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo((Text)tp.get(firstKey));
	}
    

}