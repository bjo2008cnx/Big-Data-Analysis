package myGN;

public class V_side {
	int x;
	int y;
	double value;//���value������������ż����������ĳ��Դ������Ը��ߵĽ���
	double value2;//value2��������������������еĽ���֮��
	boolean remove_if;//��GN�㷨�о���ȥ���������ıߣ���һ�������Ƿ�ȥ����ȥ�������ߵ�ʱ����ڴ�������Ϊtrue

	V_side(int x,int y, double value){
		int x1 = ( (x< y) ? x:y);
		int y1 = ( (x> y) ? x:y);
		this.x = x1;//С��ֵ��this.x
		this.y = y1;//���ֵ��this.y
		this.value = value;
		this.value2 = value;
		this.remove_if = false;//�������Ƿ����㷨��������б�ɾ����
	}
}
