import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{

	private String starting_phrase;
	private String following_word;
	private int count;
	
	public DBOutputWritable(String starting_prhase, String following_word, int count) {
		this.starting_phrase = starting_prhase;
		this.following_word = following_word;
		this.count= count;
	}

	public void readFields(ResultSet arg0) throws SQLException {

		//how to read fields?
		this.starting_phrase = arg0.getString(1);
		this.following_word = arg0.getString(2);
		this.count = arg0.getInt(3);
	}

	public void write(PreparedStatement arg0) throws SQLException {

		//how to write fields?
		arg0.setString(1, starting_phrase);
		arg0.setString(2, following_word);
		arg0.setInt(3, count);
	}

}
