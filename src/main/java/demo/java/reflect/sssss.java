package demo.java.reflect;

import java.lang.reflect.Method;

public   class   sssss
{
    public   static   void   main(String[]   args)
    {
        String   bb   =   "item1";
        String   aa   =   "get"+bb.substring(0,1).toUpperCase()+bb.substring(1,bb.length());
        try {
            Class c = Class.forName("com.reflect.test.sssss");
            Method[] m=c.getMethods();
            for (Method me:m) {
//                System.out.println(me.getName());
                if(me.getName().equals("getItem2")) {
                    System.out.println(me.getParameterCount());
                    System.out.println("result="+me.invoke(null,1,2));


                }
                
                if(me.getName().equals("getItem1")) {
                  Object invoke = me.invoke(null);
                  System.out.println(me.invoke(null));
                }
                
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    public   static   void   getItem1()   {

    System.out.print( "111111 ");
    }

    public   static   int   getItem2(int a, int b)   {

    System.out.print( "222222 ");
        return a+b;
    }
}

