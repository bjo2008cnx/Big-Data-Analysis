package com.novas.linearregression;

import java.io.IOException;
import java.util.Iterator;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class linearregression {
	//eth是参数因子，y是输出，xi是输入向量,e是学习率
	public static void refreshEth(double[] eth,double y,double[] xi,double e)
	{
		double h=0;
		for(int i=0;i<xi.length;i++)
		{
			h=h+eth[i]*xi[i];      
		}
		System.out.println("y="+(y-h));
		for(int i=0;i<eth.length;i++)
		{
			eth[i]=eth[i]+e*(y-h)*xi[i];
		}
	} 
	  public static class LinearRegressionMapper extends MapperBase {

		    Record key;
		    Record value;
		    //表示参数数组
            double[] eth;
            double alpha;
		    @Override
		    public void setup(TaskContext context) throws IOException {
		    	key=context.createMapOutputKeyRecord();
		    	value=context.createMapOutputValueRecord();
		    	alpha=context.getJobConf().getFloat("alpha",(float) 0.001);
		    }
		    
		    @Override
			public void cleanup(TaskContext context) throws IOException {
				// TODO Auto-generated method stub
		    	StringBuilder sb=new StringBuilder();
		    	int i=0;
				for( i=0;i<eth.length-1;i++)
				{
					sb.append(eth[i]).append(",");
				}
				sb.append(eth[i]);
				key.setString(0,"0");
				value.setString(0,sb.toString());
				context.write(key,value);
			}

			@Override
		    public void map(long recordNum, Record record, TaskContext context) throws IOException {
		    	System.out.println("count="+record.getColumnCount());
		    	double y=Double.valueOf(record.getString(0));
		    	String v=record.getString(1);
		    	String[] xarray=v.split(" ");
		    	double[] xi=new double[xarray.length+1];
		    	xi[0]=1;
		    	for(int i=1;i<xi.length;i++)
		    	{
		    		xi[i]=Double.valueOf(xarray[i-1]);
		    	}
                if(eth==null)
                {
                	eth=new double[xarray.length+1];
                	for(int i=0;i<eth.length;i++)
                	{
                		eth[i]=1;
                	}
                }
                refreshEth(eth,y,xi,alpha); 
                StringBuilder sb=new StringBuilder();
		    	int i=0;
				for( i=0;i<eth.length-1;i++)
				{
					sb.append(eth[i]).append(",");
				}
				sb.append(eth[i]);
                System.out.println(sb.toString());
		    }
		  }

		  /**
		   * A combiner class that combines map output by sum them.
		   */
		  public static class LinearRegressionReducer extends ReducerBase {
		    private Record result;

		    @Override
		    public void setup(TaskContext context) throws IOException {
		      result = context.createOutputRecord();
		    }

		    @Override
		    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {		       while (values.hasNext()) {
		        Record val = values.next();
		        result.setString(0,val.getString(0));
		        context.write(result);
		      }
		    }
		  }
//进行多次计算
		  public static class ReLinearRegressionMapper extends MapperBase {

			    Record key;
			    Record value;
			   
			    @Override
			    public void setup(TaskContext context) throws IOException {
			    	key=context.createMapOutputKeyRecord();
			    	value=context.createMapOutputValueRecord();
			    }
			 
				@Override
			    public void map(long recordNum, Record record, TaskContext context) throws IOException {
			        if(context.getInputTableInfo().getTableName().equals("linearregression_output"))
			        {
			        	key.setString(0,"0");
			        	value.setString(0,record.getString(0));
			        	context.write(key, value);
			        }
			        else
			        {
			        	key.setString(0,"1");
			        	value.setString(0,record.getString(0)+"_"+record.getString(1));
			        	context.write(key, value);
			        }
			    }
			  }

			  /**
			   * A combiner class that combines map output by sum them.
			   */
			  public static class ReLinearRegressionReducer extends ReducerBase {
			    private Record result;
			    //表示参数数组
	            double[] eth;
	            double alpha;
			    @Override
			    public void setup(TaskContext context) throws IOException {
			      result = context.createOutputRecord();
			      alpha=context.getJobConf().getFloat("alpha",(float) 0.001);
			     
			    }

			    @Override
				public void cleanup(TaskContext context) throws IOException {
					// TODO Auto-generated method stub
			    	StringBuilder sb=new StringBuilder();
			    	int i=0;
					for( i=0;i<eth.length-1;i++)
					{
						sb.append(eth[i]).append(",");
					}
					sb.append(eth[i]);
					result.setString(0,sb.toString());
					context.write(result);				}

				@Override
			    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {		       while (values.hasNext()) {
			      System.out.println("key="+key.getString(0));
			    	if(key.getString(0).equals("0"))
			       {
			    	   String[] var=values.next().getString(0).split(",");
			    	   eth=new double[var.length];
			    	   for(int i=0;i<var.length;i++)
			    	   {
			    		   eth[i]=Double.valueOf(var[i]);
			    	   }
			       }
			       else
			       {
			    	   while(values.hasNext())
			    	   {
			    		   Record r=values.next();
			    		   String[] v=r.getString(0).split("_");
			    		   System.out.println("v="+v);
					    	String[] xarray=v[1].split(" ");
					    	double[] xi=new double[xarray.length+1];
					    	xi[0]=1;
					    	for(int i=1;i<xi.length;i++)
					    	{
					    		xi[i]=Double.valueOf(xarray[i-1]);
					    	}
			                refreshEth(eth,Double.valueOf(v[0]),xi,alpha); 
			    	   }
			       }
			      }
			    }
			  }
			  
		  public static void main(String[] args) throws Exception {

			int count=3;  
			double alpha=0.001;
		    JobConf job = new JobConf();
		    job.setFloat("alpha",(float) alpha);
		    job.setMapperClass(LinearRegressionMapper.class);
		    job.setReducerClass(LinearRegressionReducer.class);
		    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
		    job.setMapOutputValueSchema(SchemaUtils.fromString("value:string"));
		     InputUtils.addTable(TableInfo.builder().tableName("linearregression").build(), job);
		    OutputUtils.addTable(TableInfo.builder().tableName("linearregression_output").build(), job);

		    RunningJob rj = JobClient.runJob(job);
		    rj.waitForCompletion();
		    int i=1;
		    while(i<count)
		    {
		    	i++;
		    	 JobConf Rejob = new JobConf();
		    	 Rejob.setFloat("alpha",(float) alpha);

				    Rejob.setMapperClass(ReLinearRegressionMapper.class);
				    Rejob.setReducerClass(ReLinearRegressionReducer.class);
				    Rejob.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
				    Rejob.setMapOutputValueSchema(SchemaUtils.fromString("value:string"));
				    InputUtils.addTable(TableInfo.builder().tableName("linearregression_output").build(), Rejob);
				    InputUtils.addTable(TableInfo.builder().tableName("linearregression").build(), Rejob);
				    OutputUtils.addTable(TableInfo.builder().tableName("linearregression_output").build(), Rejob);

				    rj = JobClient.runJob(Rejob);
				    rj.waitForCompletion();
		    }
		   
		  
		  }
}

