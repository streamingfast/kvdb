Running tests

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
