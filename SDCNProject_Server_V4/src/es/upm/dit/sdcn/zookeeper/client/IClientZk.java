package es.upm.dit.sdcn.zookeeper.client;

import java.util.List;

import org.apache.zookeeper.Watcher;

public interface IClientZk extends Watcher {

    public boolean createZNode(String path, byte[] data);

    public boolean createZNodeTemp(String path, byte[] data);

    public boolean setZNodeData(String path, byte[] data);

    public boolean deleteZNode(String path);

    public byte[] getZNodeData(String path);

    public List<String> listChildrenZNode(String path);

    public boolean exists(String path);

    public void closeConnection();
}
