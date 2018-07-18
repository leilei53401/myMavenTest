package mytest.collection.SynchronizedTest;

/**
 * Created by Administrator on 2018-3-20.
 */
public class MySynchronized extends Thread
{
    private String name;

    private String val;

    public MySynchronized(String name, String v)
    {
        this.name = name;
        val = v;
    }

    public void printVal()
    {
        synchronized (val)
        {
            while (true)
            {
                System.out.println(name + val);
            }
        }
    }

    public void run()
    {
        printVal();
    }

    public static void main(String args[])
    {
        MySynchronized f1 = new MySynchronized("Foo 1:", "printVal");
        f1.start();
        MySynchronized f2 = new MySynchronized("Foo 2:", "printVal");
        f2.start();
    }
}