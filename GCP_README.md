# How to deploy on k8s

## Follow the official guide 
```
helm install ./charts/tidb-cluster -n   \
  demo --namespace=tidb --set   \
  pd.storageClassName=pd-ssd,tikv.storageClassName=pd-ssd
```

## About making pd/tikv running onto different nodes

```
affinity: 
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app.kubernetes.io/component: "tikv"
        topologyKey: "kubernetes.io/hostname"
```
[More info here](https://github.com/pingcap/tidb-operator/blob/master/docs/operation-guide.md#configuration)

## Local development
```
kubectl port-forward demo-pd-0 2379:2379 -n tidb

# monitoring
kubectl port-forward demo-monitor-5ddb4bf8c-lkkwc 3000 -n tidb
```

## Run test docker in GCP k8s

```
kubectl run -it --rm --restart=Never kvrawclienttest --image=liufuyang/kvrawclienttest sh -n tidb \
  --overrides='{ "apiVersion": "v1", "spec": { "nodeSelector": { "kubernetes.io/hostname": "gke-tidb-pool-1-60d488aa-kk1v" } } }'
```