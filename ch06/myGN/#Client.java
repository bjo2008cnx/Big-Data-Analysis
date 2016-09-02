package myGN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.examples.mr.WordCount.SumCombiner;
import com.aliyun.odps.examples.mr.WordCount.SumReducer;
import com.aliyun.odps.examples.mr.WordCount.TokenizerMapper;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class Client {
	public static class TokenizerMapper extends MapperBase {
		
		int max=0;//这个是max是点的个数
		Vector<V_side> vec_relationship  = new Vector<V_side>();//记录有链接的边。
		List<String> suersns = new ArrayList<String>();
		
		 private Record result;

	    @Override
	    public void setup(TaskContext context) throws IOException {
	    	result = context.createOutputRecord();
	    }

	    @Override
	    public void map(long recordNum, Record record, TaskContext context) throws IOException {
	    	if (record.getString(1).equals("%")){
	    		max = Integer.parseInt(record.getString(0));
	    	}
	    	else {
	    		String x = record.getString(0);
	    		String y = record.getString(1);
	    		suersns.add(x+"|"+y);
	    		int xx = Integer.parseInt(x);
	    		int yy = Integer.parseInt(y);
	    		V_side node = new V_side(xx, yy, 1);
	    		vec_relationship.add(node);
	    	}
	    }
	    
	    @Override
		public void cleanup(TaskContext context) throws IOException {
	    	//当Q值最大时，Q_i存放数组result_temp[][]中对应的行号i，以便在输出结果的时候容易找到。		
			int Q_i=0;
			//result_temp_i是在存储各种Q值下分裂得到各个点的社团隶属情况要用到的行号下标变量，也就是数组result_temp[][]的行号下标，最后一次的分裂情况下的行号记录在result_temp_i中。
			int result_temp_i=0;
			//vec_result_temp数组每行下标0到max-1的是存放每个点隶属的集团号，存放每种Q 值下的分裂情况，每行对应一种情况，元素vec_result_temp.Q存放对应的Q值，		
			Vector< Point_belong> vec_result_temp = new Vector< Point_belong>(); 
			//vec_V_remove_belong数组用来存放每次去除介数最大的边的隶属情况，比如v(i,j)是在分裂为两个集团时去除的，则vec_V_remove_belong.x=i-1,vec_V_remove_belong.x=j-1，若是分裂为三个集团时去除的，则vec_V_remove_belong.belong_clique值为3
			Vector< S_remove_belong > vec_V_remove_belong = new Vector< S_remove_belong >();
			//若原始网络本身就是由几个孤立的社团构成的，original_community_alones+2是记录原始网络的孤立社团数目，
			//在vec_result_temp数组中，0到original_community_alones-1的行号记录的是原始网络中的社团检测情况。从行号为original_community_alones记录的是由于去除最大介数边的分裂情况。
			int original_community_alones=0;
			
	    	int[] r_sign =  new int[max];//记录矩阵中行号为i的元素（边）在vec_relationship 中出现的首索引
	    	for(int i = 0; i < max; i++){
				r_sign[i] = -1;
			} 
			GN_use.v_order( vec_relationship, r_sign);
			myGN GN_DEAL=new myGN(0);
			GN_DEAL.GN_deal(vec_relationship,r_sign,vec_result_temp, vec_V_remove_belong);//GN算法的入口函数
			original_community_alones=GN_DEAL.get_original_community_alones();
			Q_i=GN_DEAL.get_Q_i();
			result_temp_i=GN_DEAL.get_result_temp_i();
			if (result_temp_i>original_community_alones) {
				int final_community_num=Q_i+2;
				result.set(new Object[] {final_community_num});
		    }
			else {
				result.set(new Object[] {0});
			}
			context.write(result);
		}
	}
	
	public static void main (String[] args) throws Exception {
		JobConf job = new JobConf();
	    job.setMapperClass(TokenizerMapper.class);
	    job.setNumReduceTasks(0);
	    job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
	    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

	    InputUtils.addTable(TableInfo.builder().tableName("wc_in1").build(), job);
	    OutputUtils.addTable(TableInfo.builder().tableName("wc_out").build(), job);

	    RunningJob rj = JobClient.runJob(job);
	    rj.waitForCompletion();
	}
}
