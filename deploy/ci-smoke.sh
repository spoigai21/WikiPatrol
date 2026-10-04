#!/usr/bin/env bash
# Provision a throwaway kind cluster, deploy the stack, wait for it to be healthy, check that live
# edits flow end to end and that the classifier's autoscaler is wired to consumer lag, then tear
# the cluster down. CI runs this on every push; it runs the same way on a laptop.
#
#   deploy/ci-smoke.sh                 (needs docker, kind, kubectl; KIND/KUBECTL override paths)
set -euo pipefail

KIND=${KIND:-kind}
KUBECTL=${KUBECTL:-kubectl}
CLUSTER=${CLUSTER:-wikipatrol-ci}
KEDA_VERSION=${KEDA_VERSION:-2.21.0}
export KUBECONFIG=${KUBECONFIG:-$(mktemp -d)/kubeconfig}
k() { "$KUBECTL" "$@"; }
say() { printf '\n== %s\n' "$*"; }

cleanup() { say "tearing down"; "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

say "cluster"
"$KIND" create cluster --name "$CLUSTER" --wait 180s
say "image"
docker build -q -t wikipatrol:local .
"$KIND" load docker-image wikipatrol:local --name "$CLUSTER"

say "KEDA $KEDA_VERSION"
k apply --server-side -f "https://github.com/kedacore/keda/releases/download/v${KEDA_VERSION}/keda-${KEDA_VERSION}.yaml" >/dev/null
k -n keda rollout status deploy/keda-operator --timeout=300s
k -n keda rollout status deploy/keda-metrics-apiserver --timeout=300s

say "deploy"
k apply -k deploy/k8s
k -n wikipatrol rollout status statefulset/redpanda --timeout=300s
for d in ingester stages classifier; do k -n wikipatrol rollout status "deploy/$d" --timeout=300s; done

topic_size() { k -n wikipatrol exec redpanda-0 -- rpk topic describe "$1" -p 2>/dev/null | awk 'NR>1 {s+=$NF} END {print s+0}'; }

say "live edits flow end to end: wikimedia -> raw -> edits -> enriched -> scored -> predictions"
for i in $(seq 1 60); do
  n=$(topic_size wiki.predictions)
  [ "$n" -gt 0 ] && break
  sleep 5
done
for t in wiki.raw wiki.edits wiki.enriched wiki.scored wiki.predictions; do echo "$t: $(topic_size $t)"; done
[ "$(topic_size wiki.predictions)" -gt 0 ] || { echo "no predictions after 5 minutes"; exit 1; }

say "wiki.scored is partitioned for the classifier to scale over"
parts=$(k -n wikipatrol exec redpanda-0 -- rpk topic describe wiki.scored -p | awk 'NR>1' | wc -l | tr -d ' ')
echo "partitions: $parts"
[ "$parts" -eq 6 ] || exit 1

say "the autoscaler reads consumer lag"
for i in $(seq 1 36); do
  ready=$(k -n wikipatrol get scaledobject classifier -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
  [ "$ready" = "True" ] && break
  sleep 5
done
k -n wikipatrol get scaledobject,hpa
[ "$ready" = "True" ] || exit 1

say "every pod healthy, no restarts"
k -n wikipatrol get pods
restarts=$(k -n wikipatrol get pods -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '{s+=$1} END {print s+0}')
[ "$restarts" -eq 0 ] || { echo "$restarts container restarts"; exit 1; }
say "ok"
