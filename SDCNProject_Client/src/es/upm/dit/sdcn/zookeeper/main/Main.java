package es.upm.dit.sdcn.zookeeper.main;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import org.apache.commons.lang3.math.NumberUtils;

import es.upm.dit.sdcn.zookeeper.client.ClientZk;
import es.upm.dit.sdcn.zookeeper.client.IClientZk;

public class Main {

    private static Scanner scan;
    private static boolean exit = false;

    private static String command;
    private static String action;
    private static String option;
    private static String path;
    private static String data;
    private static String pathr;
    private static String datar;

    private static IClientZk clientZk;
    private static String hostport;

    public static void main(String[] args) throws IOException, InterruptedException {

        Scanner sc = new Scanner(System.in);
        int accNumber = 0;
        int balance = 0;
        String name = null;

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

        // 
        scan = new Scanner(System.in);

        // 
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

        // Lanzamiento de la consola
        System.out.println("\n----------------------------WELCOME TO THE BANK CLIENT----------------------------\n");
        //help(); // Muestra los comandos disponibles

        while (!exit) {
            init();
            Thread.sleep(1000);
            help();
            System.out.print("enter command > ");
            command = scan.nextLine();
            readCommand();
            boolean r = false; // 
            boolean rr = false;
            byte[] b = null; // 
            byte[] br = null; //

            switch (action) {
                case "1":
                    action = "create";
                    break;
                case "2":
                    action = "read";
                    break;
                case "3":
                    action = "update";
                    break;
                case "4":
                    action = "delete";
                    break;
                case "5":
                    action = "bankdb";
                    break;
                default:
                    break;

            }
            switch (action) {

                case "create":

                    System.out.print(">>> Enter account number (int) = ");
                    if (sc.hasNextInt()) {
                        accNumber = sc.nextInt();
                        if (!NumberUtils.isNumber(Integer.toString(accNumber))) {
                            continue;
                        } else if (accNumber <= 0) {
                            continue;
                        }
                    } else {
                        System.out.println("The provided text is not an integer");
                        sc.next();
                        continue;
                    }

                    System.out.print(">>> Enter name (String) = ");
                    name = sc.next();

                    System.out.print(">>> Enter balance (int) = ");
                    if (sc.hasNextInt()) {
                        balance = sc.nextInt();
                    } else {
                        System.out.println("The provided text is not an integer");
                        sc.next();
                        continue;
                    }

                    path = "/Create";
                    data = "INSERT INTO BANK VALUES("
                            + Integer.toString(accNumber)
                            + ",'" + name + "'," + Integer.toString(balance) + ")";

                    r = clientZk.setZNodeData(path, data.getBytes());

                    pathr = "/BankAcc" + "/" + Integer.toString(accNumber);
                    datar = name + "," + Integer.toString(balance);

                    rr = clientZk.createZNode(pathr, datar.getBytes());

                    if (r && rr) {
                        System.out.println("result command> The following account has been created : '" + Integer.toString(accNumber) + ", " + name + ", " + Integer.toString(balance) + "'");
                    } else {
                        System.err.println("result command> Error creating account");
                    }

                    break;

                case "read":
                    System.out.print(">>> Enter account number (int) = ");
                    if (sc.hasNextInt()) {
                        accNumber = sc.nextInt();
                    } else {
                        System.out.println("The provided text is not an integer");
                        sc.next();
                    }

                    path = "/Read";
                    data = "SELECT * FROM BANK WHERE ID=" + Integer.toString(accNumber);

                    r = clientZk.setZNodeData(path, data.getBytes());

                    Thread.sleep(2000);

                    // Lectura de los datos de un Znode
                    b = clientZk.getZNodeData("/ReadOk");
                    if (b != null) {
                        System.out.println("result command> Reading account data : '" + new String(b) + "'");
                    } else {
                        System.out.println("result command> path : 'null'");
                    }

                    clientZk.setZNodeData("/ReadOk", null);

                    break;

                case "update":

                    System.out.print(">>> Enter account number (int) = ");
                    if (sc.hasNextInt()) {
                        accNumber = sc.nextInt();
                        if (!NumberUtils.isNumber(Integer.toString(accNumber))) {
                            continue;
                        } else if (accNumber <= 0) {
                            continue;
                        }
                    } else {
                        System.out.println("The provided text is not an integer");
                        sc.next();
                        continue;
                    }

                    pathr = "/BankAcc" + "/" + Integer.toString(accNumber);

                    if (pathr != "" && pathr.substring(0, 1).equals("/") && clientZk.exists(pathr)) {
                        System.out.print(">>> Enter name (String) = ");
                        name = sc.next();

                        System.out.print(">>> Enter balance (int) = ");
                        if (sc.hasNextInt()) {
                            balance = sc.nextInt();
                        } else {
                            System.out.println("The provided text is not an integer");
                            sc.next();
                        }

                        path = "/Update";
                        data = "UPDATE BANK SET NAME='" + name + "', "
                                + "BALANCE=" + Integer.toString(balance)
                                + " WHERE ID=" + Integer.toString(accNumber);

                        r = clientZk.setZNodeData(path, data.getBytes());

                        datar = name + "," + Integer.toString(balance);

                        if (datar != "") {
                            rr = clientZk.setZNodeData(pathr, datar.getBytes());

                            if (r && rr) {
                                System.out.println("result command> The following account has been updated : '" + Integer.toString(accNumber) + ", " + name + ", " + Integer.toString(balance) +"'");
                            } else {
                                System.err.println("result command> Error updating account '" + Integer.toString(accNumber) + "' or account doesn't exist");
                            }
                        } else {
                            System.err.println("result command> Error updating account");
                        }
                    } else {
                        System.err.println("result command> Account doesn't exist");
                    }
                    break;

                case "delete":
                    System.out.print(">>> Enter account number (int) = ");
                    if (sc.hasNextInt()) {
                        accNumber = sc.nextInt();
                    } else {
                        System.out.println("The provided text is not an integer");
                        sc.next();
                    }

                    path = "/Delete";
                    data = "DELETE FROM BANK WHERE ID=" + Integer.toString(accNumber);

                    r = clientZk.setZNodeData(path, data.getBytes());
                    if (clientZk.exists("/List" + "/" + Integer.toString(accNumber))) {
                        // Eliminación de un Znode
                        clientZk.deleteZNode("/List" + "/" + Integer.toString(accNumber));
                    }

                    pathr = "/BankAcc" + "/" + Integer.toString(accNumber);
                    if (pathr != "" && pathr.substring(0, 1).equals("/") && clientZk.exists(pathr)) {
                        // Eliminación de un Znode
                        rr = clientZk.deleteZNode(pathr);
                    }
                    if (r && rr) {
                        System.out.println("result command> The following account has been deleted : " + Integer.toString(accNumber));
                    } else {
                        System.err.println("result command> Error deleting account");
                    }

                    break;

                case "bankdb":
                    path = "/List";
                    // mostrar la lsita de nodos con sus datos
                    data = "SELECT ID FROM BANK";

                    r = clientZk.setZNodeData(path, data.getBytes());

                    Thread.sleep(3000);

                    // mostrar la lsita de nodos con sus datos
                    List<String> ls = clientZk.listChildrenZNode(path);
                    if (ls.size() == 0) {
                        System.out.println("result command> No bank accounts have been created");
                    } else {
                        System.out.println("result command> The following account ID's have been created: " + displayList(ls));
                    }

                    break;

                case "help":
                    help();
                    break;

                case "quit":
                    exit();
                    System.out.println("QUITTING THE CLIENT");
                    break;

                default:
                    System.err.println("result command> UNAVAILABLE COMMAND");
            }
        }
    }

    private static String displayList(List<String> ls) {
        String aff = "[";
        for (int i = 0; i < ls.size(); i++) {
            if (i != 0) {
                aff += ", ";
            }
            aff += ls.get(i);
        }
        aff += "]";
        return aff;
    }

    private static void help() {
        System.out.println("AVAILABLE COMMANDS ==>");
        System.out.println("\t(1) CREATE : Create a bank account");
        System.out.println("\t(2) READ    : Read a bank account");
        System.out.println("\t(3) UPDATE    : Update a bank account");
        System.out.println("\t(4) DELETE : Delete a bank account");
        System.out.println("\t(5) BANKDB     : List all bank accounts");
        System.out.println("\tQUIT   : Exit the client");
    }

    private static void readCommand() {
        String[] cmds = command.trim().split(" ");
        action = cmds[0].toLowerCase();

        if (cmds.length == 2) {
            path = cmds[1];
        }
        if (cmds.length == 3) {
            if (cmds[1].substring(0, 1).equals("-")) {
                option = cmds[1];
                path = cmds[2];
            } else {
                path = cmds[1];
                data = cmds[2];
            }
        }
        if (cmds.length == 4) {
            option = cmds[1];
            path = cmds[2];
            data = cmds[3];
        }
    }

    private static void exit() {
        exit = true;
    }

    private static void init() {
        action = "";
        option = "";
        path = "";
        data = "";
    }
}
