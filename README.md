# Namespace-Resource-Controller

A Kubernetes controller to setup default resource requests for namespaces in a Kubernetes cluster.
Built from the [kubernetes/sample-controller](https://github.com/kubernetes/sample-controller)

## Running

To run:

```sh
 go run *.go -kubeconfig={my_kube_config} -stderrthreshold=INFO
```
