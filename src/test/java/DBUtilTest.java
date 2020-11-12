import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.nyble.util.DBUtil;
import junit.framework.TestCase;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

public class DBUtilTest extends TestCase {

    public void testBuildProps() throws ParserConfigurationException, IOException, SAXException {

        DBUtil util = DBUtil.getInstance();

        DocumentBuilderFactory factory =
                DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        StringBuilder xmlStringBuilder = new StringBuilder();
        xmlStringBuilder.append("<data_sources>" +
                "<data_source IP='1.1.1.1' PASSWORD='aaaaaa'/>" +
                "</data_sources>");
        ByteArrayInputStream input = new ByteArrayInputStream(
                xmlStringBuilder.toString().getBytes("UTF-8"));
        Document doc = builder.parse(input);

        HashMap<String, String> props = util.buildProps(doc.getElementsByTagName("data_source").item(0));

        assertTrue(props.size() == 2);
        assertEquals("1.1.1.1", props.get("IP"));
        assertEquals("aaaaaa", props.get("PASSWORD"));
    }

    public void testConnectionName(){
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDataSourceName("my_connection_app");
        cpds.setJdbcUrl("jdbc:postgresql://10.100.1.17/datawarehouse?ApplicationName=my_connection_app");
        cpds.setUser("postgres");
        cpds.setPassword("postgres10@");
        try(Connection conn = cpds.getConnection()){
            Thread.sleep(1000*30);
        } catch (InterruptedException | SQLException e) {
            e.printStackTrace();
        }
    }
}
