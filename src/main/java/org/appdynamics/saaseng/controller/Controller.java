package org.appdynamics.saaseng.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.autoscaling.v1.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import org.appdynamics.saaseng.Main;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Controller {

    /* Init */
    KubernetesClient client;
    SharedIndexInformer<HorizontalPodAutoscaler> horizontalPodAutoscalerSharedIndexInformer;
    SharedIndexInformer<Pod> podSharedIndexInformer;
    BlockingQueue<String> workQueue;
    Lister<HorizontalPodAutoscaler> horizontalPodAutoscalerLister;
    Lister<Pod> podLister;
    String namespace;

        /* Read events from HPA */
    final static String kafka_hpa=System.getenv("KAFKA_HPA_NAME");
    public static Logger logger = Logger.getLogger(Controller.class.getName());

    public Controller(String namespace, KubernetesClient client, SharedIndexInformer<HorizontalPodAutoscaler> horizontalPodAutoscalerSharedIndexInformer,
                      SharedIndexInformer<Pod> podSharedIndexInformer)
    {
        this.client=client;
        this.namespace=namespace;
        this.horizontalPodAutoscalerSharedIndexInformer=horizontalPodAutoscalerSharedIndexInformer;
        this.podSharedIndexInformer=podSharedIndexInformer;

        this.horizontalPodAutoscalerLister=new Lister<>(horizontalPodAutoscalerSharedIndexInformer.getIndexer(),namespace);
        this.podLister=new Lister<>(podSharedIndexInformer.getIndexer(),namespace);

        workQueue=new ArrayBlockingQueue<>(1024);
    }

    public void create()
    {
        /* Check horizontal pod autoscaler */
        horizontalPodAutoscalerSharedIndexInformer.addEventHandler(new ResourceEventHandler<HorizontalPodAutoscaler>() {
            @Override
            public void onAdd(HorizontalPodAutoscaler horizontalPodAutoscaler) {
                enqueueHpa(horizontalPodAutoscaler);
            }

            @Override
            public void onUpdate(HorizontalPodAutoscaler horizontalPodAutoscaler, HorizontalPodAutoscaler newHpa) {
                enqueueHpa(newHpa);
            }

            @Override
            public void onDelete(HorizontalPodAutoscaler horizontalPodAutoscaler, boolean b) {

            }
        });
    }

    public void run() {

        /* Custom Resoruce Definition */

        CustomResourceDefinitionContext customResourceDefinitionContext = new CustomResourceDefinitionContext.Builder()
                .withVersion("v1beta1")
                .withName("kafkas.kafka.strimzi.io")
                .withGroup("kafka.strimzi.io")
                .withKind("Kafka")
                .withPlural("kafkas")
                .withScope("Namespaced")
                .build();


        String kafkaClusterName = System.getenv("KAFKA_CLUSTER_NAME");

        /* Wait for Sync to get done */
        while(!horizontalPodAutoscalerSharedIndexInformer.hasSynced());

        while(true)
        {
            while(!workQueue.isEmpty())
            {
                try {
                    String key = workQueue.take();
                    if (key == null || key.isEmpty() || !key.contains("/"))
                        throw new InterruptedException();

                    String name = key.split("/")[1];
                    HorizontalPodAutoscaler horizontalPodAutoscaler = horizontalPodAutoscalerLister.get(name);

                    if(horizontalPodAutoscaler==null)
                    {
                        logger.log(Level.SEVERE,"HPA is null");
                        return;
                    }
                    reconcile(customResourceDefinitionContext, horizontalPodAutoscaler, kafkaClusterName);
                }
                catch (InterruptedException e)
                {
                    System.out.println("Error : "+e.getMessage());
                }
            }

        }
    }

    private void reconcile(CustomResourceDefinitionContext customResourceDefinitionContext, HorizontalPodAutoscaler horizontalPodAutoscaler, String kafkaClusterName)
    {
        int hpaDesiredReplicas = horizontalPodAutoscaler.getStatus().getDesiredReplicas();
        int hpaCurrentReplicas = horizontalPodAutoscaler.getStatus().getCurrentReplicas();

        Map<String, Object> kafkaCluster = client.customResource(customResourceDefinitionContext).get(namespace, kafkaClusterName);
        int kafkasCurrentReplicas=((HashMap<Object, Integer>)((HashMap<Object, Object>)kafkaCluster.get("spec")).get("kafka")).get("replicas");

        logger.log(Level.INFO,"The number of Current/Desired KAFKA replicas are ::  "+kafkasCurrentReplicas+"/"+hpaDesiredReplicas);

        if(hpaCurrentReplicas==hpaDesiredReplicas && hpaDesiredReplicas==kafkasCurrentReplicas) {
            logger.log(Level.INFO,"No real changes need to be made, HPA and Kafka Replicas are the same. "+hpaCurrentReplicas+"/"+hpaDesiredReplicas);
            return;
        }
        else {
            logger.log(Level.INFO,"Updating the Kafka CRD with the right replicas :: "+hpaCurrentReplicas+"/"+hpaDesiredReplicas);
            updateKafkaWithRightReplicas(customResourceDefinitionContext, hpaDesiredReplicas, kafkaCluster, kafkaClusterName);
        }
    }

    private void updateKafkaWithRightReplicas(CustomResourceDefinitionContext customResourceDefinitionContext, int desiredKafkaReplicas,
                                              Map<String, Object> kafkaCluster, String kafkaClusterName)
    {
        try {

            kafkaCluster = client.customResource(customResourceDefinitionContext).get(namespace, kafkaClusterName);

            if (kafkaCluster != null) {
                logger.log(Level.INFO, "Found custom resource ! ");

                ((HashMap<Object, Integer>) ((HashMap<String, Object>) (kafkaCluster.get("spec"))).get("kafka")).put("replicas", desiredKafkaReplicas);

                client.customResource(customResourceDefinitionContext).edit(namespace, kafkaClusterName, new ObjectMapper().writeValueAsString(kafkaCluster));
                logger.log(Level.INFO, "Changed Number of Replicas to "+desiredKafkaReplicas);
            } }
            catch(Exception err) {
                logger.log(Level.SEVERE, "Error "+err);
            }

    }

    private void enqueueHpa(HorizontalPodAutoscaler horizontalPodAutoscaler)
    {

        String key= Cache.metaNamespaceKeyFunc(horizontalPodAutoscaler);

        if(key != null || !key.isEmpty())
        {
            if(key.split("/")[1].equals(kafka_hpa)) {
                workQueue.add(key);
                logger.log(Level.INFO,"A change has been made to HPA :: "+key);
            }
        }
    }
}
