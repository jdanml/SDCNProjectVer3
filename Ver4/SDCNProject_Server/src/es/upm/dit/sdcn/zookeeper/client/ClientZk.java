package es.upm.dit.sdcn.zookeeper.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ClientZk implements IClientZk {

    private ZooKeeper zk;

    public ClientZk(String hostPort) {
        try {
            zk = new ZooKeeper(hostPort, 5000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean createZNode(String path, byte[] data) {
        try {
            zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return true;
        } catch (KeeperException e) {
            //e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            //e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean createZNodeTemp(String path, byte[] data) {
        try {
            zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } catch (KeeperException e) {
            //e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            //e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean setZNodeData(String path, byte[] data) {
        try {
            zk.setData(path, data, zk.exists(path, true).getVersion());
            return true;
        } catch (KeeperException e) {
            //e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            //e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean deleteZNode(String path) {
        try {
            zk.delete(path, zk.exists(path, true).getVersion());
            return true;
        } catch (KeeperException e) {
            //e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            //e.printStackTrace();
            return false;
        }
    }

    @Override
    public byte[] getZNodeData(String path) {
        byte[] data = null;

        try {
            data = zk.getData(path, true, null);
        } catch (KeeperException e) {
            //e.printStackTrace();
        } catch (InterruptedException e) {
            //e.printStackTrace();
        }
        return data;
    }

    @Override
    public List<String> listChildrenZNode(String path) {
        List<String> result = new ArrayList<>();
        try {
            result = zk.getChildren(path, false);
        } catch (KeeperException e) {
            //e.printStackTrace();
        } catch (InterruptedException e) {
            //e.printStackTrace();
        }
        return result;
    }

    @Override
    public boolean exists(String path) {
        try {
            Stat stat = zk.exists(path, false);
            if (stat != null) {
                return true;
            } else {
                return false;
            }
        } catch (KeeperException e) {
            //e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            //e.printStackTrace();
            return false;
        }
    }

    @Override
    public void closeConnection() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            //e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
    }

}
