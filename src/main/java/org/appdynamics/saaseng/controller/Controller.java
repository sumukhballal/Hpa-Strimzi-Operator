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
import org.appdynamics.saaseng.crd.Kafkas;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Controller {

    /* Init */
    KubernetesClient client;
    SharedIndexInformer<Kafkas> kafkasSharedIndexInformer;
    SharedIndexInformer<HorizontalPodAutoscaler> horizontalPodAutoscalerSharedIndexInformer;
    SharedIndexInformer<Pod> podSharedIndexInformer;
    BlockingQueue<String> workQueue;
    Lister<HorizontalPodAutoscaler> horizontalPodAutoscalerLister;
    Lister<Pod> podLister;
    String namespace;

        /* Read events from HPA */
    final static String kafka_hpa="kafka-hpa";
    public static Logger logger = Logger.getLogger(Controller.class.getName());

    public Controller(String namespace, KubernetesClient client, SharedIndexInformer<HorizontalPodAutoscaler> horizontalPodAutoscalerSharedIndexInformer,
                      SharedIndexInformer<Pod> podSharedIndexInformer)
    {
        this.client=client;
        this.namespace=namespace;
        this.horizontalPodAutoscalerSharedIndexInformer=horizontalPodAutoscalerSharedIndexInformer;
        //this.kafkasSharedIndexInformer=kafkasSharedIndexInformer;
        this.podSharedIndexInformer=podSharedIndexInformer;

        this.horizontalPodAutoscalerLister=new Lister<>(horizontalPodAutoscalerSharedIndexInformer.getIndexer(),namespace);
        this.podLister=new Lister<>(podSharedIndexInformer.getIndexer(),namespace);

        workQueue=new ArrayBlockingQueue<>(1024);
    }

    public void create()
    {
        /*
        kafkasSharedIndexInformer.addEventHandler(new ResourceEventHandler<Kafkas>() {
            @Override
            public void onAdd(Kafkas kafkas) {
                enqueue(kafkas);
            }

            @Override
            public void onUpdate(Kafkas kafkas, Kafkas newkafkas) {
                enqueue(newkafkas);
            }

            @Override
            public void onDelete(Kafkas kafkas, boolean b) {

            }
        }); */

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
    /*
        podSharedIndexInformer.addEventHandler(new ResourceEventHandler<Pod>() {
            @Override
            public void onAdd(Pod pod) {
                handlePodObject(pod);
            }

            @Override
            public void onUpdate(Pod pod, Pod newpod) {
                handlePodObject(newpod);
            }

            @Override
            public void onDelete(Pod pod, boolean b) {

            }
        }); */

    }

    public void run() {
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
                        System.out.println("Error : HPA is null");
                        return;
                    }

                    reconcile(horizontalPodAutoscaler);
                }
                catch (InterruptedException e)
                {
                    System.out.println("Error : "+e.getMessage());
                }
            }

        }
    }

    private void reconcile(HorizontalPodAutoscaler horizontalPodAutoscaler)
    {
        int desiredReplicas = horizontalPodAutoscaler.getStatus().getDesiredReplicas();
        int currentReplicas = horizontalPodAutoscaler.getStatus().getCurrentReplicas();


        logger.log(Level.INFO,"The number of Current/Desired replicas are ::  "+currentReplicas+"/"+desiredReplicas);
        if(currentReplicas==desiredReplicas)
        {
            logger.log(Level.INFO,"No real changes need to be made :: "+currentReplicas+"/"+desiredReplicas);
            //updateKafkaWithRightReplicas(desiredReplicas);
            return;
        }
        else
        {
            logger.log(Level.INFO,"Updating the Kafka CRD with the right replicas :: "+currentReplicas+"/"+desiredReplicas);
            updateKafkaWithRightReplicas(desiredReplicas);
        }
    }

    private void updateKafkaWithRightReplicas(int desiredKafkaReplicas)
    {

        CustomResourceDefinitionContext customResourceDefinitionContext = new CustomResourceDefinitionContext.Builder()
                .withVersion("v1beta1")
                .withName("kafkas.kafka.strimzi.io")
                .withGroup("kafka.strimzi.io")
                .withKind("Kafka")
                .withPlural("kafkas")
                .withScope("Namespaced")
                .build();
        try {
            Map<String, Object> kafkaCluster = client.customResource(customResourceDefinitionContext).get(namespace, "my-cluster");

            if (kafkaCluster != null) {
                logger.log(Level.INFO, "Found custom resource ! ");

                ((HashMap<Object, Integer>) ((HashMap<String, Object>) (kafkaCluster.get("spec"))).get("kafka")).put("replicas", desiredKafkaReplicas);

                client.customResource(customResourceDefinitionContext).edit(namespace, "my-cluster", new ObjectMapper().writeValueAsString(kafkaCluster));

                logger.log(Level.INFO, "Changed Number of Replicas to "+desiredKafkaReplicas);
                //((HashMap<LinkedHashMap, Integer>) ((HashMap<String, LinkedHashMap>) (kafkaCluster.get("spec"))).get("kafka")).get("replicas");


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
    /*
    private void handlePodObject(Pod pod)
    {
        OwnerReference ownerReference = getOwnerReference_Util(pod);
        if(!ownerReference.getKind().equalsIgnoreCase("HorizontalPodAutoscaler"))
            return;

        /* Listers to get owner reference of Pods */ /*
        HorizontalPodAutoscaler horizontalPodAutoscaler=horizontalPodAutoscalerLister.get(ownerReference.getName());
        if(horizontalPodAutoscaler!=null)
            enqueue(horizontalPodAutoscaler);
    }*/

    private void createPods(int replicas, Object o)
    {

    }

    private OwnerReference getOwnerReference_Util(Pod pod)
    {
        for( OwnerReference reference : pod.getMetadata().getOwnerReferences())
        {
            if(reference.getController().equals(Boolean.TRUE))
                return reference;
        }
        return null;
    }

    public void getHpaResource(KubernetesClient client, String namespace)
    {
        List<HorizontalPodAutoscaler> horizontalPodAutoscalerList=client.autoscaling().v1().horizontalPodAutoscalers().list().getItems();

    }

    public void getCustomResourceDefinition(KubernetesClient client)
    {

    }
}
