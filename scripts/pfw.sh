#!/usr/bin/env bash

kubectl -n demo-system port-forward svc/events-presenter 8084:8084