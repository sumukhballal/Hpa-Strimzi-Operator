package org.appdynamics.saaseng.crd;

import io.fabric8.kubernetes.client.CustomResource;

public class Kafkas extends CustomResource {

    private KafkasSpec kafkasSpec;
    private KafkasStatus kafkasStatus;
}
