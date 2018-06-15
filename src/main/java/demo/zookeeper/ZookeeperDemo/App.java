package demo.zookeeper.ZookeeperDemo;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	int first = Integer.MAX_VALUE; 
    	int second = Integer.MAX_VALUE; 
    	int[] list = new int[]{2,5,1,6};
    	
    	for(int num:list) {
    		if(num<=first) {
    			first = num;
    			continue;
    		}
    		if(first<num && num<second) {
    			second = num;
    			continue;
    		}
    		if(num>second) {
    			System.out.println("true");
    		}
    		
    		
    	}
    	System.out.println("false");
    }
}
