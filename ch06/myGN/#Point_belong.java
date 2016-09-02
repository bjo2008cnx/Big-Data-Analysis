package myGN;

public class Point_belong {
	double Q;//记录这种分裂情况下的Q值。
	int []point_belong_to;//记录这种分裂情况下，每个点的属于哪个集团。例如点0属于集团2，则有point_belong_to[0]=2
	
	Point_belong(int num, double Q){
		this.Q = Q;
		this.point_belong_to = new int[num];
	}
}
