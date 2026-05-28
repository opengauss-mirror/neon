#!/usr/bin/env bash

# A basic test to ensure Docker images are built correctly.
# Build a wrapper around the compute, start all services and runs a simple SQL query.
# Repeats the process for all currently supported openGauss versions.

# Implicitly accepts `REPOSITORY` and `TAG` env vars that are passed into the compose file
# Their defaults point at DockerHub `neondatabase/neon:latest` image.`,
# to verify custom image builds (e.g pre-published ones).
#
set -eux -o pipefail

cd "$(dirname "${0}")"
export COMPOSE_FILE='docker-compose.yml'
export PARALLEL_COMPUTES=${PARALLEL_COMPUTES:-1}
export NEON_IMAGE=${NEON_IMAGE:-neon:latest_opgs}
export COMPUTE_IMAGE=${COMPUTE_IMAGE:-compute-node-opengauss-v702:latest}
READY_MESSAGE="All computes are started"
COMPUTES=()
for i in $(seq 1 "${PARALLEL_COMPUTES}"); do
  COMPUTES+=("compute${i}")
done
CURRENT_TMPDIR=$(mktemp -d)
trap 'rm -rf ${CURRENT_TMPDIR} docker-compose-parallel.yml' EXIT
if [[ ${PARALLEL_COMPUTES} -gt 1 ]]; then
  export COMPOSE_FILE=docker-compose-parallel.yml
  cp docker-compose.yml docker-compose-parallel.yml
  # Replace the environment variable PARALLEL_COMPUTES with the actual value
  yq eval -i ".services.compute_is_ready.environment |=  map(select(. | test(\"^PARALLEL_COMPUTES=\") | not)) + [\"PARALLEL_COMPUTES=${PARALLEL_COMPUTES}\"]" ${COMPOSE_FILE}
  for i in $(seq 2 "${PARALLEL_COMPUTES}"); do
    # Duplicate compute1 as compute${i} for parallel execution
    yq eval -i ".services.compute${i} = .services.compute1" ${COMPOSE_FILE}
    # We don't need these sections, so delete them
    yq eval -i "(del .services.compute${i}.build) | (del .services.compute${i}.ports) | (del .services.compute${i}.networks)" ${COMPOSE_FILE}
    # Let the compute 1 be the only dependence
    yq eval -i ".services.compute${i}.depends_on = [\"compute1\"]" ${COMPOSE_FILE}
    # Set RUN_PARALLEL=true for compute2. They will generate tenant_id and timeline_id to avoid using the same as other computes
    yq eval -i ".services.compute${i}.environment += [\"RUN_PARALLEL=true\"]" ${COMPOSE_FILE}
    # Remove TENANT_ID and TIMELINE_ID from the environment variables of the generated computes
    # They will create new TENANT_ID and TIMELINE_ID anyway.
    yq eval -i ".services.compute${i}.environment |= map(select(. | (test(\"^TENANT_ID=\") or test(\"^TIMELINE_ID=\")) | not))" ${COMPOSE_FILE}
  done
fi

function cleanup() {
    echo "show container information"
    docker ps
    echo "stop containers..."
    docker compose down
}

for og_version in ${TEST_VERSION_ONLY-V702}; do
    echo "clean up containers if exist"
    cleanup
    OG_VERSION=${og_version} docker compose up --quiet-pull -d
    echo "wait until the compute is ready. timeout after 60s. "
    cnt=0
    while sleep 3; do
        # check timeout
        (( cnt += 3 ))
        if [[ ${cnt} -gt 60 ]]; then
            echo "timeout before the compute is ready."
            exit 1
        fi
        if docker compose logs compute_is_ready | grep -q "${READY_MESSAGE}"; then
            echo "OK. The compute is ready to connect."
            echo "execute simple insert queries."
            for compute in "${COMPUTES[@]}"; do
              docker compose exec "${compute}" /bin/bash -c "gsql -d postgres -U cloud_admin -p 55433 -h localhost -c \"DROP TABLE IF EXISTS docker_compose_insert_test; CREATE TABLE docker_compose_insert_test(id int primary key, note text); INSERT INTO docker_compose_insert_test VALUES (1, 'ok'); SELECT * FROM docker_compose_insert_test;\""
            done
            break
        fi
    done
done
