package generator;


import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class SchemaGenerator {
  private List<RowGenerator> rgs = new ArrayList<RowGenerator>();
  public long scale = 1;
  public int addRowGenerator(RowGenerator rg) {
    rgs.add(rg);
    return 0;
  }

  public int setScale(int scale) {
    this.scale = scale;
    return 0;
  }


  public void generateData(Properties props) throws Exception {
    // how to set expected rows for each table
    for (RowGenerator rg : rgs) {
      rg.generateData(props);
    }
  }

  public static void main(String[] args) {

    String sql = "create table test (a int, b STRING, c decimal(7,2)) PARTITIONED BY (d decimal(5,0))";

    String sql_841 = "create TABLE tbl_data_event_1d ( " +
      "record_id DECIMAL(23,0) not null, " +
      "cdr_id STRING, location_code DECIMAL(6,0) not null, system_id DECIMAL(5,0), clue_id STRING, hit_element STRING, carrier_code DECIMAL(2,0), " +
      "device_id STRING, cap_time TIMESTAMP, data_character STRING, netcell_id STRING, client_mac STRING, server_mac STRING, tunnel_type STRING, " +
      "tunnel_ip_client STRING, tunnel_ip_server STRING, tunnel_id_client STRING, tunnel_id_server STRING, side_one_tunnel_id STRING, " +
      "side_two_tunnel_id STRING, client_ip STRING, server_ip STRING, trans_protocol STRING, client_port DECIMAL(5,0), server_port DECIMAL(5,0), " +
      "app_protocol STRING, client_area DECIMAL(3,0), server_area DECIMAL(3,0), language STRING, stype STRING, summary STRING, file_type STRING, " +
      "filename STRING, filesize STRING, bill_type DECIMAL(6,0), ori_user_num STRING, user_num STRING, user_imsi STRING, user_imei STRING, " +
      "user_belong_area_code STRING, user_belong_country_code DECIMAL(10,0), user_longitude DECIMAL(20,8), user_latitude DECIMAL(20,8), " +
      "user_msc STRING, user_base_station STRING, user_curr_area_code STRING, user_curr_country_code DECIMAL(10,0), user_signal_point STRING, " +
      "user_ip STRING, ori_oppo_num STRING, oppo_num STRING, oppo_imsi STRING, oppo_imei STRING, oppo_belong_area_code STRING, " +
      "oppo_belong_country_code DECIMAL(10,0), oppo_longitude DECIMAL(20,8), oppo_latitude DECIMAL(20,8), oppo_msc STRING, oppo_base_station STRING," +
      " oppo_curr_area_code STRING, oppo_curr_country_code DECIMAL(10,0), oppo_signal_point STRING, oppo_ip STRING, ring_time TIMESTAMP, " +
      "call_estab_time TIMESTAMP, end_time TIMESTAMP, call_duration DECIMAL(10,0), call_status_code DECIMAL(3,0), dtmf STRING, ori_other_num STRING," +
      " other_num STRING, roam_num STRING, send_time TIMESTAMP, ori_sms_content STRING, ori_sms_code DECIMAL(3,0), sms_content STRING, " +
      "sms_num DECIMAL(2,0), sms_count DECIMAL(2,0), remark STRING, content_status DECIMAL(6,0), voc_length DECIMAL(10,0), fax_page_count " +
      "DECIMAL(6,0), com_over_cause DECIMAL(6,0), roam_type DECIMAL(1,0), sgsn_ad STRING, ggsn_ad STRING, pdp_ad STRING, apn_ni STRING, " +
      "apn_oi STRING, card_id STRING, time_out DECIMAL(1,0), on_time TIMESTAMP, load_id DECIMAL(22,0) )";

    try {
      SchemaGenerator sg = new SchemaGenerator();
      RowGenerator rg = new RowGenerator(sql_841);
      rg.setExpectedRows(sg.scale * 1024 * 1024 / rg.getBytesInRow());
      sg.addRowGenerator(rg);
      String project_root = System.getProperty("user.dir");
      FileInputStream fis = new FileInputStream(project_root + "/conf/datagen.properties");
      Properties props = new Properties();
      props.load(fis);
      sg.generateData(props);
      fis.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
