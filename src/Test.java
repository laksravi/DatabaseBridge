import java.util.ArrayList;


public class Test {

		public boolean someX(int P, ArrayList<Integer> my){
			if(P==0)
				return true;
			ArrayList<Integer> newarr = new ArrayList<Integer>();
			
			if(someX(P-1, newarr))
					my.addAll(newarr);
			System.out.println(newarr.size());
			my.add(3);
			
			return P%2==0;
		}
		
	
	public static void main(String[] args){
			Test t = new Test();
			ArrayList<Integer> s= new ArrayList<Integer>();
			t.someX(10,s );
			System.out.println(s.size());
	}
}
