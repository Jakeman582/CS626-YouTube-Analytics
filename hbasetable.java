import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

public class CreateTable {
      
   public static void main(String[] args) throws IOException {

      // Instantiating configuration class
      Configuration con = HBaseConfiguration.create();

      // Instantiating HbaseAdmin class
      HBaseAdmin admin = new HBaseAdmin(con);

      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new
      HTableDescriptor(TableName.valueOf("videosData"));

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("uploader_common_name"));
      tableDescriptor.addFamily(new HColumnDescriptor("uploader_username"));
      tableDescriptor.addFamily(new HColumnDescriptor("video_title"));
      tableDescriptor.addFamily(new HColumnDescriptor("number_of_viewers"));
      tableDescriptor.addFamily(new HColumnDescriptor("number_of_likes"));
      tableDescriptor.addFamily(new HColumnDescriptor("number_of_dislikes"));
      tableDescriptor.addFamily(new HColumnDescriptor("number_of_comments"));

      // Execute the table through admin
      admin.createTable(tableDescriptor);
      System.out.println(" Table created ");
   }
}