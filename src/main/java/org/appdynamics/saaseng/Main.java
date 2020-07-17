package org.appdynamics.saaseng;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.autoscaling.v1.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.api.model.autoscaling.v1.HorizontalPodAutoscalerList;
import io.fabric8.kubernetes.api.model.rbac.*;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.appdynamics.saaseng.controller.Controller;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static Logger logger = Logger.getLogger(Main.class.getName());


    public static void main(String[] args) {
        try(KubernetesClient client = new DefaultKubernetesClient())
        {
            String namespace=client.getNamespace();
            if (namespace == null) {
                logger.log(Level.INFO, "No namespace found via config, assuming default.");
                namespace = System.getenv("KUBERNETES_NAMESPACE");
            }

            logger.log(Level.INFO,"Current namespace is :: "+namespace);

            /* Create a Cluster Role Binding -- To be done, for now it is created on cluster manually */
            //Main main=new Main();
            //main.createClusterRoleBinding(client,namespace);
            /* Create and subscribe to events from a certain HPA */

            /* Get all informers into factory */
            SharedInformerFactory factory=client.informers();
            /* Create a shared Index informer for HPA with resync every 10 mins */
            SharedIndexInformer<HorizontalPodAutoscaler> horizontalPodAutoscalerSharedIndexInformer=factory.sharedIndexInformerFor(HorizontalPodAutoscaler.class,
                    HorizontalPodAutoscalerList.class, 10*60*1000);

            /* Create a shared Index Informer for PODS with resync every 10 mins */
            SharedIndexInformer<Pod> podSharedIndexInformer=factory.sharedIndexInformerFor(Pod.class,
                    PodList.class, 10*60*1000);

            /* start controller */

            Controller controller = new Controller(namespace, client, horizontalPodAutoscalerSharedIndexInformer, podSharedIndexInformer);
            controller.create();

            factory.startAllRegisteredInformers();

            /* Run Controller */

            controller.run();
        }
        catch (KubernetesClientException e) {
            logger.log(Level.SEVERE, "Client Exception : {}"+ e.getMessage());
        }
    }

    private void createClusterRoleBinding(KubernetesClient client, String namespace) {
        ClusterRoleBinding kubernetesClusterRoleBinding = new ClusterRoleBindingBuilder()
                .withNewMetadata()
                .withName("default-pod")
                .endMetadata()
                .addToSubjects(0, new SubjectBuilder()
                        .withApiGroup("rbac.authorization.k8s.io")
                        .withKind("ServiceAccount")
                        .withName("default")
                        .withNamespace(namespace)
                        .build()
                )
                .withRoleRef(new RoleRefBuilder()
                        .withApiGroup("rbac.authorization.k8s.io")
                        .withKind("ClusterRole")
                        .withName("cluster-admin")
                        .build()
                ).build();

        client.rbac().clusterRoleBindings().create(kubernetesClusterRoleBinding);


    }
}
