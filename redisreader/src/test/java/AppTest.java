
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;


/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }
    
    String json="{\"alipay_no\": \"ALI\",          \"available_confirm_fee\": \"0.00\",                 \"buyer_memo\": \"\",          \"buyer_message\": \"\",          \"buyer_nick\": \"鱼\",          \"buyer_obtain_point_fee\": 21,          \"buyer_rate\": 0,          \"cod_fee\": \"0.00\",          \"cod_status\": \"NEW_CREATED\",          \"commission_fee\": \"1.84\",          \"consign_time\": \"2015-02-26T13:34:35+08:00\",          \"consign_time_time\": \"13:34:35\",          \"create_time_\": \"2015-02-23T10:40:57+08:00\",          \"created\": \"2015-02-23T10:40:57+08:00\",          \"created_day\": \"2015-02-23\",          \"created_time\": \"10:40:57\",          \"discount_fee\": \"0.00\",          \"end_time\": \"2015-03-01T22:06:28+08:00\",          \"end_time_time\": \"22:06:28\",          \"is_has_post_fee\": 0,          \"kd_consign_company\": \"百世汇通\",          \"kd_receive_day\": \"2015-02-28\",          \"kd_receive_mobile\": \"18850555520\",          \"kd_receive_province\": \"福建省\",          \"kd_receive_status\": 1,          \"kd_receive_time\": \"2015-02-28T12:20:15+08:00\",          \"kd_receive_time_time\": \"12:20:15\",          \"kd_refund_fee\": \"0.00\",          \"kd_refund_status\": 0,          \"kd_wwid\": \"\",          \"member_grade\": 0,          \"modify_time\": \"2015-03-01T22:06:28+08:00\",          \"num\": 7      }";
    
    public void testInsertRedis(){
    	Jedis client = getRedisClient();
    	Long ts = System.currentTimeMillis();
    	Pipeline pp = client.pipelined();
    	pp.multi();
    	for(int i=0;i<1000;i++){
    		
    		pp.rpush("JSON_TEST", json.replace("ALI", String.valueOf(i)));
    	}
    	pp.exec();
    	System.out.println(((System.currentTimeMillis()-ts)/1000)+ "s, insert ready!");
    	client.close();
    }
    
    public void testReadingRedis(){
    	Jedis client = getRedisClient();
    	Long ts = System.currentTimeMillis();
    	List<String> json=null;
    	int size = 110;
    	do{
    		json = client.lrange("JSON_TEST", 0, size-1);
    		client.ltrim("JSON_TEST", size, -1);
    		
    		if(json.size()>0){
    			System.out.println(json.get(0)+" - "+json.get(json.size()-1));
    		}
    		System.out.println(json.size());
    	}while(json!=null&&!json.isEmpty());
    	
    	
    	
    	System.out.println(((System.currentTimeMillis()-ts)/1000)+ "s, read finish!");
    	client.close();
    }
    
    public Jedis getRedisClient(){
    	// 生成连接池配置信息
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(10);
		config.setMaxTotal(30);
		config.setMaxWaitMillis(5*1000);
		//config.setTestOnBorrow(true);  

		//redis.host=120.26.99.223
		//		redis.port=6333
		//		redis.pass=kdcloud@Nascent_2017
		// 在应用初始化的时候生成连接池
		Jedis client= new JedisPool(config, "127.0.0.1", 6333,30000)
				.getResource();
		client.auth("zero");
		return client;
    }
}
