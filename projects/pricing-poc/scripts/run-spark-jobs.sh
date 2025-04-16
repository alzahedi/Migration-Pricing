#!/bin/bash
#
#
#       Script to spark submit the various jobs in the spark-orchestrator
#       project.
#

export GIT_ROOT=$(git rev-parse --show-toplevel)
export PRICING_POC_DIR="${GIT_ROOT}/projects/pricing-poc"

# Remove checkpoint directory if it exists
CHECKPOINT_DIR="${PRICING_POC_DIR}/src/main/resources/output/eventhub-checkpoint"
if [ -d "$CHECKPOINT_DIR" ]; then
    echo "Removing existing checkpoint directory: $CHECKPOINT_DIR"
    rm -rf "$CHECKPOINT_DIR"
fi

get_spark_configs() {
    spark_resource_configs+=("--conf" "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file://${PRICING_POC_DIR}/log4j2.properties")
    spark_resource_configs+=("--conf" "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=file://${PRICING_POC_DIR}/log4j2.properties")
    echo "${spark_resource_configs[@]}"
}

export spark_job_config=$(get_spark_configs)
/opt/spark/bin/spark-submit ${spark_job_config[@]} --class example.StreamDriver --master local[*] target/scala-2.12/pricing-poc-assembly-0.1.0-SNAPSHOT.jar

