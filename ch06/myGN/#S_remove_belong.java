package myGN;

public class S_remove_belong {
	int x;//�ߵĵ�ֵx��y��
	int y;
	int belong_clique;//���������ڷ���Ϊ�������ŵ�ʱ��ɾ�����ġ�����v(i,j)���ڷ���Ϊ��������ʱȥ����,��x=i,y=j;belong_clique=2;

	S_remove_belong(int x, int y, int belong_clique){
		this.x = x;
		this.y = y;
		this.belong_clique = belong_clique;
	}
}
