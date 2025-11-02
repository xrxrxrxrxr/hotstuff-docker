#!/usr/bin/env bash
# Pin sequencing verify threads to dedicated CPU cores via cgroup v2

set -euo pipefail

CGROUP_ROOT="/sys/fs/cgroup"
HOTSTUFF_CG="${CGROUP_ROOT}/hotstuff"
MEDIAN_CG="${HOTSTUFF_CG}/verify-median"
COMBINED_CG="${HOTSTUFF_CG}/verify-combined"

MEDIAN_CPU="2"
COMBINED_CPU="3"
DEFAULT_CPUS="0-1"
DEFAULT_MEM="0"

if [[ $EUID -ne 0 ]]; then
    exec sudo "$0" "$@"
fi

if [[ ! -d ${CGROUP_ROOT} ]]; then
    echo "[cgroup] cgroup root ${CGROUP_ROOT} does not exist" >&2
    exit 1
fi

if [[ ! -f ${CGROUP_ROOT}/cgroup.controllers ]]; then
    echo "[cgroup] cgroup v2 controllers not available" >&2
    exit 1
fi

mkdir -p "${HOTSTUFF_CG}" "${MEDIAN_CG}" "${COMBINED_CG}"

if ! grep -q "+cpuset" "${CGROUP_ROOT}/cgroup.subtree_control"; then
    echo "+cpuset" > "${CGROUP_ROOT}/cgroup.subtree_control" || true
fi

echo "${DEFAULT_CPUS}" > "${HOTSTUFF_CG}/cpuset.cpus"
echo "${DEFAULT_MEM}" > "${HOTSTUFF_CG}/cpuset.mems"

PID=$(pgrep -f docker_node | head -n1 || true)
if [[ -z "${PID}" ]]; then
    echo "[cgroup] docker_node process not found" >&2
    exit 1
fi

echo "[cgroup] assigning docker_node pid ${PID} to ${HOTSTUFF_CG} (cpus ${DEFAULT_CPUS})"
echo "${PID}" > "${HOTSTUFF_CG}/cgroup.procs"

echo "${MEDIAN_CPU}" > "${MEDIAN_CG}/cpuset.cpus"
echo "${DEFAULT_MEM}" > "${MEDIAN_CG}/cpuset.mems"
echo "${COMBINED_CPU}" > "${COMBINED_CG}/cpuset.cpus"
echo "${DEFAULT_MEM}" > "${COMBINED_CG}/cpuset.mems"

MEDIAN_TID=$(ps -eL -p "${PID}" | awk '/multisig_verify_median/ {print $2; exit}')
if [[ -z "${MEDIAN_TID}" ]]; then
    echo "[cgroup] median verify thread not found (yet). Retrying for up to 5s..."
    for _ in {1..5}; do
        sleep 1
        MEDIAN_TID=$(ps -eL -p "${PID}" | awk '/multisig_verify_median/ {print $2; exit}')
        [[ -n "${MEDIAN_TID}" ]] && break
    done
fi
if [[ -n "${MEDIAN_TID}" ]]; then
    echo "[cgroup] placing median verify thread ${MEDIAN_TID} on CPU ${MEDIAN_CPU}"
    echo "${MEDIAN_TID}" > "${MEDIAN_CG}/cgroup.threads"
else
    echo "[cgroup] median verify thread not detected. Run script again later." >&2
fi

COMBINED_TID=$(ps -eL -p "${PID}" | awk '/multisig_verify_combined/ {print $2; exit}')
if [[ -z "${COMBINED_TID}" ]]; then
    echo "[cgroup] combined verify thread not found (yet). Retrying for up to 5s..."
    for _ in {1..5}; do
        sleep 1
        COMBINED_TID=$(ps -eL -p "${PID}" | awk '/multisig_verify_combined/ {print $2; exit}')
        [[ -n "${COMBINED_TID}" ]] && break
    done
fi
if [[ -n "${COMBINED_TID}" ]]; then
    echo "[cgroup] placing combined verify thread ${COMBINED_TID} on CPU ${COMBINED_CPU}"
    echo "${COMBINED_TID}" > "${COMBINED_CG}/cgroup.threads"
else
    echo "[cgroup] combined verify thread not detected. Run script again later." >&2
fi

echo "[cgroup] configuration complete"
