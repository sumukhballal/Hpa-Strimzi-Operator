package org.appdynamics.saaseng.crd;

public class KafkasSpec {
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    private int replicas;
}
