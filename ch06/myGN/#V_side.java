package myGN;

public class V_side {
	int x;
	int y;
	double value;//这个value数组是用来存放计算过程中以某个源点出发对各边的介数
	double value2;//value2数组是用来存放最终所有的介数之和
	boolean remove_if;//在GN算法中决定去掉介数最大的边，这一项设置是否去掉，去掉这条边的时候对于此项设置为true

	V_side(int x,int y, double value){
		int x1 = ( (x< y) ? x:y);
		int y1 = ( (x> y) ? x:y);
		this.x = x1;//小的值给this.x
		this.y = y1;//大的值给this.y
		this.value = value;
		this.value2 = value;
		this.remove_if = false;//这条边是否在算法处理过程中被删除掉
	}
}
