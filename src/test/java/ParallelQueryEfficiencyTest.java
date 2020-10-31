import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.nyble.util.AutoCommitFalseConnCustomizer;
import com.nyble.util.DBUtil;
import com.nyble.util.consumerActionQ.AdvancedConsumerActionQuery;
import com.nyble.util.consumerActionQ.Column;
import com.nyble.util.consumerActionQ.DateAndAction;
import com.nyble.util.consumerActionQ.ParallelConsumerActionQuery;
import junit.framework.TestCase;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class ParallelQueryEfficiencyTest extends TestCase {

    private ComboPooledDataSource ds;
    private DBUtil instance;
    @Override
    protected void setUp() throws Exception {
        ds = new ComboPooledDataSource();
        ds.setJdbcUrl("jdbc:postgresql://localhost/hcrm");
        ds.setUser("btalpiga");
        ds.setPassword("root");
        ds.setMaxPoolSize(60);
        ds.setInitialPoolSize(20);
        ds.setAcquireIncrement(5);
        ds.setConnectionCustomizerClassName(AutoCommitFalseConnCustomizer.class.getName());
        instance = mock(DBUtil.class);
        when(instance.getConnection()).then(new Answer<Connection>() {

            @Override
            public Connection answer(InvocationOnMock invocationOnMock) throws Throwable {
                return ds.getConnection();
            }
        }); //thenReturn(ds.getConnection());
        when(instance.getConnection(
                any(String.class)
        )).thenReturn(ds.getConnection());

        Field instanceField = DBUtil.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, instance);

    }

    @Override
    protected void tearDown() throws Exception {
        ds.close();
    }


    public void testSimple() throws Exception {
        Date from = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01").getTime());
        int[] actions = new int[]{1723,1725,1730,1738,1741,1744,1747,1750,1753,1755,1757,1759,1761,1763,1766,1767,1772,1775,1776,1782,1784,1786,1797,1804,1805,1807,1808,1813,1816,1829,1831,1832,1833,1834,1836,1841,1855,1856,1860,1877,1878,1881,1885,1890,1891,1907,1922,1927,1932,1940,1942,1943,1948,1951,1955,1959,1960,1964,2013,2015,2016,2017,2019,2032,2038,2044,2047,2048,2050,2056,2057,2058,2059,2060,2061,2063,2064,2065,2066,2067,2068,2069,2071,2072,2073,2074,2075,2077,2078,2079,2080,2083,2084,2085,2086,2087,2088,2089,2090,2092,2093,2094,2098,2099,3015,3016,3017,3018,3019,3020,3021,3022,3023,3024,3025,3026,3027,3028,3029,3030,3031,3032,3033,3034,3035,3037,3038,3039,3040,3041,3042,3043,3045,3046,3049,3051,3052,3053,3054,3055,3058,3059,3060,3061,3062,3064,3065,3066,3067,3068,3069,3070,3071,3072,3073,3074,3075,3076,3077,3078,3079,3080,3081,3082,3083,3084,3088,3089,3090,3091,3092,3096,3097,3098,3099,3100,3101,3102,3105,3106,3107};
        Date to = new Date(new java.util.Date().getTime());
        List<DateAndAction> daa = new ArrayList<>();
        for(int act : actions){
            daa.add(new DateAndAction(from, to, act));
        }

        ArrayList<Column> fields = new ArrayList<>();
        fields.add(new Column(Types.INTEGER,"id",true));
        fields.add(new Column(Types.INTEGER,"id_consumer",false));
        fields.add(new Column(Types.DATE, "date::DATE", false));
        fields.add(new Column(Types.INTEGER, "id_action", false));
        AdvancedConsumerActionQuery query = new ParallelConsumerActionQuery(daa,"", fields, 20, null);

        //full scan one partition in year 2020
        AtomicInteger cnt = new AtomicInteger();
        long start = System.currentTimeMillis();
        query.query("1=1 ", row->{
            //do nothing
            cnt.incrementAndGet();
        });
        query.close();
        long end = System.currentTimeMillis();
        System.out.println("Processed "+cnt.get()+" in "+ (end-start) +" ms");
    }

    public void testRunQuery() throws SQLException {
        printMem();
        try(Connection conn = instance.getConnection();
            Statement st = conn.createStatement()){
            st.setFetchSize(1000);
            ResultSet rs = st.executeQuery("SELECT * FROM partitions.consumer_action_partition_4");
            printMem();
//            st.setFetchSize(1000);
            int cnt = 0;
            while(rs.next()){
                cnt++;
            }
            System.out.print("Queried "+cnt+" ");
            printMem();
        }
    }



    private void printMem(){
        // Get current size of heap in bytes
        long heapSize = Runtime.getRuntime().totalMemory();

        // Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
        long heapMaxSize = Runtime.getRuntime().maxMemory();

        // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
        long heapFreeSize = Runtime.getRuntime().freeMemory();
        System.out.println(String.format("current heap size %d bytes, max is %d bytes, free is %d bytes",
                heapSize,heapMaxSize, heapFreeSize));
    }

//    public void testSimple_whereClause() throws Exception {
//        Date from = new Date(new SimpleDateFormat("dd-MM-yyyy").parse("2020-01-01").getTime());
//        Date to = new Date(new java.util.Date().getTime());
//        List<DateAndAction> daa = Arrays.asList(
//                new DateAndAction(from, to, 1814),
//                new DateAndAction(from, to, 1815)
//        );
//
//        ArrayList<Column> fields = new ArrayList<>();
//        fields.add(new Column(Types.INTEGER,"id",true));
//        fields.add(new Column(Types.INTEGER,"id_consumer",false));
//        fields.add(new Column(Types.DATE, "date::DATE", false));
//        fields.add(new Column(Types.INTEGER, "id_action", false));
//        AdvancedConsumerActionQuery query = new ParallelConsumerActionQuery(daa,"", fields, 20);
//
//        //full scan on actions 1814 and 1815 in year 2020
//        AtomicInteger cnt = new AtomicInteger();
//        long start = System.currentTimeMillis();
//        query.query("1 = 1 ", row->{
//            //do nothing
//            cnt.incrementAndGet();
//        });
//        query.close();
//        long end = System.currentTimeMillis();
//        System.out.println("Processed "+cnt.get()+" in "+ (end-start) +" ms");
//    }
//
//    public void testSimple_whereJoin() throws Exception {
//        Date from = new Date(new SimpleDateFormat("dd-MM-yyyy").parse("2020-01-01").getTime());
//        Date to = new Date(new java.util.Date().getTime());
//        List<DateAndAction> daa = Arrays.asList(
//                new DateAndAction(from, to, 1814),
//                new DateAndAction(from, to, 1815)
//        );
//
//        ArrayList<Column> fields = new ArrayList<>();
//        fields.add(new Column(Types.INTEGER,"id",true));
//        fields.add(new Column(Types.INTEGER,"id_consumer",false));
//        fields.add(new Column(Types.DATE, "date::DATE", false));
//        fields.add(new Column(Types.INTEGER, "id_action", false));
//        AdvancedConsumerActionQuery query = new ParallelConsumerActionQuery(daa,"", fields, 20);
//
//        //full scan on actions 1814 and 1815 in year 2020
//        AtomicInteger cnt = new AtomicInteger();
//        long start = System.currentTimeMillis();
//        query.query("", row->{
//            //do nothing
//            cnt.incrementAndGet();
//        });
//        query.close();
//        long end = System.currentTimeMillis();
//        System.out.println("Processed "+cnt.get()+" in "+ (end-start) +" ms");
//    }

}
