package org.in.hadoop.ch2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable {
	
	private Date min = new Date();
	private Date max = new Date();
	private long count = 0;
	
	public final static SimpleDateFormat DATE_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	

	/**
	 * @return the minimum date
	 */
	public Date getMin() {
		return min;
	}

	/**
	 * @param min set the minimum date
	 */
	public void setMin(Date min) {
		this.min = min;
	}

	/**
	 * @return the maximum date
	 */
	public Date getMax() {
		return max;
	}

	/**
	 * @param max set the max date
	 */
	public void setMax(Date max) {
		this.max = max;
	}

	/**
	 * @return the count
	 */
	public long getCount() {
		return count;
	}

	/**
	 * @param count set the count 
	 */
	public void setCount(long count) {
		this.count = count;
	}

    /**
     * Write the data out in order it is read,
	 * using the UNIX timestamp to represent the Date	
     */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);
	}

	/**
	 * Read the Data out in order it is written,
	 * creating new Date objects the UNIX timestamp
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();		
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return DATE_FORMAT.format(min) + "\t" 
	         + DATE_FORMAT.format(max) + "\t"
	         + count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (count ^ (count >>> 32));
		result = prime * result + ((max == null) ? 0 : max.hashCode());
		result = prime * result + ((min == null) ? 0 : min.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MinMaxCountTuple other = (MinMaxCountTuple) obj;
		if (count != other.count)
			return false;
		if (max == null) {
			if (other.max != null)
				return false;
		} else if (max.compareTo(other.max) != 0)
			return false;
		if (min == null) {
			if (other.min != null)
				return false;
		} else if (min.compareTo(other.min) != 0)
			return false;
		return true;
	}
	
	

}
