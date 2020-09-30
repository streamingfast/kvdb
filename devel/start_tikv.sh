#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

main() {
  pushd "$ROOT" &> /dev/null

  while getopts "h" opt; do
    case $opt in
      h) usage && exit 0;;
      \?) usage_error "Invalid option: -$OPTARG";;
    esac
  done
  shift $((OPTIND-1))

  set -e
  echo "Starting Kubernetes cluster (via MiniKube)"
  minikube start

  # Will ensure 'kubectl' points to the right location (enforced in 'minikube status')
  minikube update-context
  minikube status

  kubectl apply -f https://raw.githubusercontent.com/tikv/tikv-operator/master/manifests/crd.v1beta1.yaml
  helm repo add pingcap https://charts.pingcap.org/

  kubectl create ns tikv-operator
  helm install -n tikv-operator tikv-operator pingcap/tikv-operator --version v0.1.0

  kubectl create ns tikv-cluster
  kubectl -n tikv-cluster apply -f https://raw.githubusercontent.com/tikv/tikv-operator/master/examples/basic/tikv-cluster.yaml
  echo ""

  echo "Waiting for TiKV cluster to be ready, can take a few minutes (needs to pull Docker images)"
  kubectl -n tikv-cluster wait --for=condition=Ready --timeout 10m tikvcluster/basic
  echo ""

  echo "Deployed, checking that everything is deployed correctly"
  kubectl -n tikv-cluster get tikvcluster/basic
  kubectl -n tikv-cluster get pods -o wide
  echo ""

  echo "All good, you are ready to run your tests".
  echo ""
  echo "First ensure that your 'kubectl'  points to the created cluster above before continuing"
  echo "... you wouldn't want to peform all this on the production cluster right!"
  echo ""
  echo "Then add those two elements to your '/etc/hosts' file:"
  echo ""
  echo "\`\`\`"
  echo "# For TiKV local operator (via k8s in minikube)"
  echo "127.0.0.1 basic-pd-0.basic-pd-peer.tikv-cluster.svc"
  echo "127.0.0.1 basic-tikv-0.basic-tikv-peer.tikv-cluster.svc"
  echo "\`\`\`"
  echo ""
  echo "Port forward some of the deployed services to your localhost interface:"
  echo ""
  echo "kubectl -n tikv-cluster port-forward svc/basic-pd 2379:2379"
  echo "kubectl -n tikv-cluster port-forward svc/basic-tikv-peer 20160:20160"
  echo ""
  echo "You are now ready to run the tests, use this command to run the tests:"
  echo ""
  echo "TEST_TIKV=tikv://127.0.0.1:2379/data-{prefix} go test ./store/tikv/...  # {prefix} will be replace dynamically be a unique value upon execution"
  echo ""
}

usage_error() {
  message="$1"
  exit_code="$2"

  echo "ERROR: $message"
  echo ""
  usage
  exit ${exit_code:-1}
}

usage() {
  echo "usage: start_tikv.sh <option>"
  echo ""
  echo "Starts a TiKV cluster using minikube, so it spins up a Kubernetes cluster"
  echo "using Minikube and then runs all required commands to setup and launch a"
  echo "local TiKV cluster."
  echo ""
  echo "You need to have Minikube installed for this script to work as well as"
  echo "Helm (v3) since it's used. This script does not check nor install those"
  echo "for now."
  echo ""
  echo "Options"
  echo "    -h          Display help about this script"
}

main "$@"