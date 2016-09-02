package myGN;

public class S_remove_belong {
	int x;//边的点值x和y。
	int y;
	int belong_clique;//这条边是在分裂为几个集团的时候被删除掉的。比如v(i,j)是在分裂为两个集团时去除的,则x=i,y=j;belong_clique=2;

	S_remove_belong(int x, int y, int belong_clique){
		this.x = x;
		this.y = y;
		this.belong_clique = belong_clique;
	}
}
