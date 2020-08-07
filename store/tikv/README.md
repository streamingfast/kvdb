## Running tests

### Docker Compose

Boot up a local `tikv` cluster:

    git clone https://github.com/pingcap/tidb-docker-compose
    cd tibd-docker-compose
    docker-compose up

Then, extract the IPs for the different components, and put them in your `/etc/hosts` file:

    docker inspect tidbdockercompose_tikv0_1| grep IPAddress | tail -n 1
    docker inspect tidbdockercompose_tikv1_1| grep IPAddress | tail -n 1
    docker inspect tidbdockercompose_tikv2_1| grep IPAddress | tail -n 1
    docker inspect tidbdockercompose_pd0_1| grep IPAddress | tail -n 1
    docker inspect tidbdockercompose_pd1_1| grep IPAddress | tail -n 1
    docker inspect tidbdockercompose_pd2_1| grep IPAddress | tail -n 1

and put the corresponding IPs in your `hosts` file:

    172.19.0.10  tikv2
    172.19.0.8   tikv1
    172.19.0.9   tikv0
    172.19.0.4   pd0
    172.19.0.7   pd1
    172.19.0.2   pd2

Then, your tests can talk to the cluster. Tadam! This will work on Linux, not sure on a Mac.

Surely, there's a better way.

### Minikube + TiKV Operator

Here what's worked best for me, the Docker Composer nor the Docker Stack version did not
work properly.

Ensure you have installed:
- minikube (or have access to a Kubernetes cluster, install with `brew install minikube` on OS X)
- helm (v3) (install script `curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash`)

This is extracted from https://tikv.org/docs/4.0/tasks/try/tikv-operator/#step-2-deploy-tikv-operator but
giving here as a quick succession of steps without explanation

```
minikube start # If you don't have a local configured cluster

# Ensure your kubectl points to the created cluster above
kubectl apply -f https://raw.githubusercontent.com/tikv/tikv-operator/master/manifests/crd.v1beta1.yaml

helm repo add pingcap https://charts.pingcap.org/

kubectl create ns tikv-operator
helm install -n tikv-operator tikv-operator pingcap/tikv-operator --version v0.1.0

kubectl create ns tikv-cluster
kubectl -n tikv-cluster apply -f https://raw.githubusercontent.com/tikv/tikv-operator/master/examples/basic/tikv-cluster.yaml

# Can take a long time (needs to pull Docker images)
kubectl -n tikv-cluster wait --for=condition=Ready --timeout 10m tikvcluster/basic

# Manual checks
kubectl -n tikv-cluster get tikvcluster/basic
kubectl -n tikv-cluster get pods -o wide
```

Once the cluster is up, add those two elements to your `/etc/hosts` file:

```
# For TiKV local operator (via k8s in minikube)
127.0.0.1 basic-pd-0.basic-pd-peer.tikv-cluster.svc
127.0.0.1 basic-tikv-0.basic-tikv-peer.tikv-cluster.svc
```

Finally open two port forward using `kubectl`:

```
kubectl -n tikv-cluster port-forward svc/basic-pd 2379:2379
kubectl -n tikv-cluster port-forward svc/basic-tikv-peer 20160:20160
```

You should now be able to run the tests:

```
TEST_TIKV=tikv://127.0.0.1:2379/data go test ./store/tikv/...
```
