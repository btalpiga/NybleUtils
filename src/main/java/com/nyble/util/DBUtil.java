package com.nyble.util;

import java.io.*;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class DBUtil {

    private static DBUtil instance;

    private final Map<String, ComboPooledDataSource> dataSources = new HashMap<>();
    private final Map<String, Map<String, String>> dataSourcesProperties = new HashMap<>();

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not load postgres driver!");
        }
    }

    private DBUtil ()  {
        parseDataSourceConfigFiles();
    }

    public static DBUtil getInstance()  {
        if(instance == null){
            instance = new DBUtil();
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        if(dataSources.size()>1){
            throw new RuntimeException("There are multiple datasources! Please provide the ds name!");
        }
        return dataSources.values().iterator().next().getConnection();
    }
    public Connection getConnection(String dataSourceName) throws SQLException {
        if(dataSourceName == null || dataSourceName.isEmpty()){
            return getConnection();
        }
        return dataSources.get(dataSourceName).getConnection();
    }

    public boolean checkIfRelationExists(String relName, String dataSourceName){
        if(relName.contains(".")){
            relName = relName.substring(relName.lastIndexOf(".")+1);
        }

        try(Connection conn = getConnection(dataSourceName);
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select * from pg_class where relname = '"+relName+"'")){
            return rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean checkIfRelationExists(String relName){
        if(relName.contains(".")){
            relName = relName.substring(relName.lastIndexOf(".")+1);
        }

        try(Connection conn = getConnection();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select * from pg_class where relname = '"+relName+"'")){
            return rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    private void parseDataSourceConfigFiles(){

            try(InputStream is = DBUtil.class.getClassLoader().getResourceAsStream("conf/DataSources.ds")){

                loadDataSource(is);

            } catch (Exception e) {
                e.printStackTrace();
            }
            if(dataSources.size() == 0){
                System.out.println("[WARNING] No datasource provided!");
            }

    }

    public void loadDataSource(InputStream is) throws IOException, SAXException, ParserConfigurationException {

        DocumentBuilderFactory factory =
                DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(is);
        NodeList dataSourcesFromXml = doc.getElementsByTagName("data_source");
        for(int i=0; i<dataSourcesFromXml.getLength(); i++){
            Node dataSource = dataSourcesFromXml.item(i);
            HashMap<String, String> props = buildProps(dataSource);
            if(!props.containsKey("JDBC_URL") || !props.containsKey("USERNAME") || !props.containsKey("PASSWORD")
                    || !props.containsKey("DS_NAME")){
                System.out.println("DataSource not properly configured!");
                return ;
            }else{
                dataSourcesProperties.put(props.get("DS_NAME"), props);
                ComboPooledDataSource ds = new ComboPooledDataSource();
                String jdbcUrl = props.get("JDBC_URL");
                if(props.containsKey("CONNECTION_NAME")) {
                    jdbcUrl+= "?ApplicationName="+props.get("CONNECTION_NAME");
                } else{
                    jdbcUrl+= "?ApplicationName="+props.get("DS_NAME");
                }
                ds.setJdbcUrl(jdbcUrl);
                ds.setUser(props.get("USERNAME"));
                ds.setPassword(props.get("PASSWORD"));
                ds.setPreferredTestQuery("SELECT 1");
                dataSources.compute(props.get("DS_NAME"), (key, val)->{
                    if(val!=null){
                        val.close();
                    }
                    return ds;
                });
                if(props.containsKey("MAX_POOL_SIZE")){
                    ds.setMaxPoolSize(Integer.parseInt(props.get("MAX_POOL_SIZE")));
                }

                if(props.containsKey("INITIAL_POOL_SIZE")){
                    ds.setInitialPoolSize(Integer.parseInt(props.get("INITIAL_POOL_SIZE")));
                }

                if(props.containsKey("ACQUIRE_INCREMENT")){
                    ds.setAcquireIncrement(Integer.parseInt(props.get("ACQUIRE_INCREMENT")));
                }

                if(props.containsKey("AUTOCOMMIT")){
                    if(props.get("AUTOCOMMIT").equalsIgnoreCase("FALSE")){
                        ds.setConnectionCustomizerClassName(AutoCommitFalseConnCustomizer.class.getName());
                    }
                }

                ds.setIdleConnectionTestPeriod(300);//test idle connections after 300 sec
                ds.setTestConnectionOnCheckin(true);
            }
        }
    }

    public HashMap<String, String> buildProps(Node dataSource) {
        HashMap<String, String> rez = new HashMap<>();
        NamedNodeMap attrs = dataSource.getAttributes();
        for(int i=0;i<attrs.getLength(); i++){
            rez.put(attrs.item(i).getNodeName(), attrs.item(i).getNodeValue());
        }

        return rez;
    }


    public Map<String, String> getDataSourceProperties(String dsName){
        return new HashMap<>(dataSourcesProperties.get(dsName));
    }

}
