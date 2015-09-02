package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class insert {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		String tableName = "yablogs";
		if (args.length < 2) {

			System.out.println("Usage, params:");
			System.out.println(" param1 - \"add\" for add records");
			System.err.println(" param1 - \"del\" or \"delete\" - for delete records");
			System.err.println(" param2 - count of records for adding");

		} else if (args[0].equals("add") && !args[1].isEmpty()) {

			int maxI = Integer.valueOf(args[1]);

			long startMs = System.currentTimeMillis();

			System.out.print("Adding... ");

			// Instantiating Configuration class
			Configuration config = HBaseConfiguration.create();

			// Instantiating HTable class
			HTable hTable = new HTable(config, tableName);

			// Instantiating Put class
			// accepts a row name.

			for (int i = 1; i <= maxI; i++) {

				Put p = new Put(Bytes.toBytes("row" + Integer.toString(i)));

				// adding values using add() method
				// accepts column family name, qualifier/row name ,value
				p.add(Bytes.toBytes("yablog"), Bytes.toBytes("name"), Bytes.toBytes("Имя " + Integer.toString(i)));
				p.add(Bytes.toBytes("yablog"), Bytes.toBytes("city"), Bytes.toBytes("hyderabad" + Integer.toString(i)));
				p.add(Bytes.toBytes("yablog"), Bytes.toBytes("designation"),
						Bytes.toBytes("manager" + Integer.toString(i)));
				p.add(Bytes.toBytes("yablog"), Bytes.toBytes("salary"), Bytes.toBytes(String.valueOf(50000 * i)));

				// Saving the put Instance to the HTable.
				hTable.put(p);
			}
			System.out.println("Data inserted! Sec: " + ((System.currentTimeMillis() - startMs) / 1000));

			// closing HTable
			hTable.close();
		} else if ((args[0].equals("delete") || args[0].equals("del")) && !args[1].isEmpty()) {

			int maxI = Integer.valueOf(args[1]);

			long startMs = System.currentTimeMillis();
			System.out.print("Deletion... ");
			// Instantiating Configuration class
			Configuration config = HBaseConfiguration.create();

			@SuppressWarnings("resource")
			HTable table = new HTable(config, tableName);
			List<Delete> list = new ArrayList<Delete>();

			for (int i = 1; i <= maxI; i++) {

				Delete del = new Delete(("row" + Integer.toString(i)).getBytes());
				list.add(del);
				table.delete(list);
				// System.out.println("Record - " + ("row" +
				// Integer.toString(i)) + " - deleted");

			}

			System.out.println("Data deeted! Sec: " + ((System.currentTimeMillis() - startMs) / 1000));

		} else {

			System.out.println("Usage, params:");
			System.out.println(" param1 - \"add\" for add records");
			System.err.println(" param1 - \"del\" or \"delete\" - for delete records");
			System.err.println(" param2 - count of records for adding");

		}

	}

}
