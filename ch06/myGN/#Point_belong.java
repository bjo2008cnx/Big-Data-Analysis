package myGN;

public class Point_belong {
	double Q;//��¼���ַ�������µ�Qֵ��
	int []point_belong_to;//��¼���ַ�������£�ÿ����������ĸ����š������0���ڼ���2������point_belong_to[0]=2
	
	Point_belong(int num, double Q){
		this.Q = Q;
		this.point_belong_to = new int[num];
	}
}
