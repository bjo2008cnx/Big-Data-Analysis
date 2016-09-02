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
		
		int max=0;//�����max�ǵ�ĸ���
		Vector<V_side> vec_relationship  = new Vector<V_side>();//��¼�����ӵıߡ�
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
	    	//��Qֵ���ʱ��Q_i�������result_temp[][]�ж�Ӧ���к�i���Ա�����������ʱ�������ҵ���		
			int Q_i=0;
			//result_temp_i���ڴ洢����Qֵ�·��ѵõ�������������������Ҫ�õ����к��±������Ҳ��������result_temp[][]���к��±꣬���һ�εķ�������µ��кż�¼��result_temp_i�С�
			int result_temp_i=0;
			//vec_result_temp����ÿ���±�0��max-1���Ǵ��ÿ���������ļ��źţ����ÿ��Q ֵ�µķ��������ÿ�ж�Ӧһ�������Ԫ��vec_result_temp.Q��Ŷ�Ӧ��Qֵ��		
			Vector< Point_belong> vec_result_temp = new Vector< Point_belong>(); 
			//vec_V_remove_belong�����������ÿ��ȥ���������ıߵ��������������v(i,j)���ڷ���Ϊ��������ʱȥ���ģ���vec_V_remove_belong.x=i-1,vec_V_remove_belong.x=j-1�����Ƿ���Ϊ��������ʱȥ���ģ���vec_V_remove_belong.belong_cliqueֵΪ3
			Vector< S_remove_belong > vec_V_remove_belong = new Vector< S_remove_belong >();
			//��ԭʼ���籾������ɼ������������Ź��ɵģ�original_community_alones+2�Ǽ�¼ԭʼ����Ĺ���������Ŀ��
			//��vec_result_temp�����У�0��original_community_alones-1���кż�¼����ԭʼ�����е����ż����������к�Ϊoriginal_community_alones��¼��������ȥ���������ߵķ��������
			int original_community_alones=0;
			
	    	int[] r_sign =  new int[max];//��¼�������к�Ϊi��Ԫ�أ��ߣ���vec_relationship �г��ֵ�������
	    	for(int i = 0; i < max; i++){
				r_sign[i] = -1;
			} 
			GN_use.v_order( vec_relationship, r_sign);
			myGN GN_DEAL=new myGN(0);
			GN_DEAL.GN_deal(vec_relationship,r_sign,vec_result_temp, vec_V_remove_belong);//GN�㷨����ں���
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
