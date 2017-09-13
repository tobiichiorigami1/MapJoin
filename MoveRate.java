package MapJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MoveRate implements WritableComparable<MoveRate>{
    private String movieid;
    private String userid;
    private int rate;
    private String movieName;
    private String movietype;
    private long ts;
    
	public MoveRate(String movieid, String userid, int rate, String movieName,
			String movietype, long ts) {
		super();
		this.movieid = movieid;
		this.userid = userid;
		this.rate = rate;
		this.movieName = movieName;
		this.movietype = movietype;
		this.ts = ts;
	}
	public MoveRate() {
		super();
		// TODO Auto-generated constructor stub
	}
	public String getMovieid() {
		return movieid;
	}
	public void setMovieid(String movieid) {
		this.movieid = movieid;
	}
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public int getRate() {
		return rate;
	}
	public void setRate(int rate) {
		this.rate = rate;
	}
	public String getMovieName() {
		return movieName;
	}
	public void setMovieName(String movieName) {
		this.movieName = movieName;
	}
	public String getMovietype() {
		return movietype;
	}
	public void setMovietype(String movietype) {
		this.movietype = movietype;
	}
	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	public int compareTo(MoveRate o) {
		// TODO Auto-generated method stub
		if(o.movieid.compareTo(this.movieid)!=0){
			return o.movieid.compareTo(this.movieid);
		}
		else {
			return o.userid.compareTo(this.userid);
		}
	}
	public void readFields(DataInput in) throws IOException {
	       this.movieid=in.readUTF();
	       this.userid=in.readUTF();
	       this.rate=in.readInt();
	       this.movieName=in.readUTF();
	       this.movietype=in.readUTF();
	       this.ts=in.readLong();
		
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		   out.writeUTF(movieid);
		   out.writeUTF(userid);
		   out.writeInt(rate);
		   out.writeUTF(movieName);
		   out.writeUTF(movietype);
		   out.writeLong(ts);
	}

}
