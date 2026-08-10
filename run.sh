#!/bin/bash
# run.sh - provisioning only.
# Benchmarking is no longer run through Ansible; use benchmark.sh or
# rclone_benchmark.sh directly from the client after this completes.
export ANSIBLE_FORCE_COLOR=1
export PY_COLORS=1
ansible-playbook -i inventory.ini playbook.yml