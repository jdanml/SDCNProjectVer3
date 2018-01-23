package es.upm.dit.sdcn.zookeeper.main;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import es.upm.dit.sdcn.zookeeper.client.ClientZk;
import es.upm.dit.sdcn.zookeeper.client.IClientZk;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class Main {

    private static IClientZk clientZk;
    private static String hostport;

    public static void main(String[] args) throws Exception, IOException, InterruptedException {

        if (args.length > 1) {
            System.err.println("Just 1 argument [host:port]");
            System.exit(2);
        }
        // terminal

        // 
        if (args.length == 0) {
            hostport = "localhost:2181";
        } else {
            hostport = args[0];
        }

        clientZk = new ClientZk(hostport);

        if (!clientZk.exists("/BankAcc")) {
            clientZk.createZNode("/BankAcc", null);
        }
        if (!clientZk.exists("/Create")) {
            clientZk.createZNode("/Create", null);
        }
        if (!clientZk.exists("/Read")) {
            clientZk.createZNode("/Read", null);
        }
        if (!clientZk.exists("/ReadOk")) {
            clientZk.createZNode("/ReadOk", null);
        }
        if (!clientZk.exists("/Update")) {
            clientZk.createZNode("/Update", null);
        }
        if (!clientZk.exists("/Delete")) {
            clientZk.createZNode("/Delete", null);
        }
        if (!clientZk.exists("/List")) {
            clientZk.createZNode("/List", null);
        }

        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection(
                "jdbc:h2:tcp://localhost/~/test", "sa", null);

        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS BANK");
        stmt.execute("CREATE TABLE BANK(ID INT PRIMARY KEY, NAME VARCHAR2(255), BALANCE INT);");
        byte[] b = null;
        String q = new String();

        String path = "/BankAcc";
        // mostrar la lsita de nodos con sus datos
        List<String> ls = clientZk.listChildrenZNode(path);
        String[] bsplit;
        for (int i = 0; i < ls.size(); i++) {
            path = "/BankAcc" + "/" + ls.get(i);
            // Lectura de los datos de un Znode
            b = clientZk.getZNodeData(path);
            if (b != null) {
                bsplit = new String(b).split(",");
                q = "INSERT INTO BANK VALUES("
                        + ls.get(i)
                        + ",'" + bsplit[0] + "'," + bsplit[1] + ")";
                stmt.execute(q);
            }
        }

        while (true) {
            byte[] c = null;
            c = clientZk.getZNodeData("/Create");
            if (c != null) {
                try {
                    stmt.executeUpdate(new String(c));
                    Thread.sleep(1000);
                    c = null;
                } catch (SQLException se) {
                    se.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    clientZk.setZNodeData("/Create", null);
                }
            }

            byte[] r = null;
            r = clientZk.getZNodeData("/Read");
            if (r != null) {
                try {
                    ResultSet rs = stmt.executeQuery(new String(r));
                    while (rs.next()) {
                        int id = rs.getInt("ID");
                        String name = rs.getString("NAME");
                        int bal = rs.getInt("BALANCE");
                        String dataRead = Integer.toString(id) + "," + name + "," + Integer.toString(bal);
                        clientZk.setZNodeData("/ReadOk", dataRead.getBytes());
                    }
                    Thread.sleep(1000);
                    clientZk.setZNodeData("/Read", null);
                    r = null;
                } catch (SQLException se) {
                    //se.printStackTrace();
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }

            byte[] u = null;
            u = clientZk.getZNodeData("/Update");
            if (u != null) {
                try {
                    stmt.executeUpdate(new String(u));
                    Thread.sleep(1000);
                    clientZk.setZNodeData("/Update", null);
                    u = null;
                } catch (SQLException se) {
                    se.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            byte[] d = null;
            d = clientZk.getZNodeData("/Delete");
            if (d != null) {
                try {
                    stmt.executeUpdate(new String(d));
                    Thread.sleep(1000);
                    clientZk.setZNodeData("/Delete", null);
                    d = null;
                } catch (SQLException se) {
                    se.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            byte[] l = null;
            l = clientZk.getZNodeData("/List");
            if (l != null) {
                try {
                    ResultSet rsl = stmt.executeQuery(new String(l));
                    ResultSetMetaData rsmd = rsl.getMetaData();
                    int columnsNumber = rsmd.getColumnCount();
                    while (rsl.next()) {
                        for (int i = 1; i <= columnsNumber; i++) {
                            String columnValue = rsl.getString(i);
                            path = "/List" + "/" + columnValue;
                            clientZk.createZNode(path, null);
                        }
                    }
                    l = null;
                } catch (SQLException se) {
                    //se.printStackTrace();
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }

            //stmt.close();
            //conn.close();
        }
    }
}
