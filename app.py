#!/usr/bin/env python3

from aws_cdk import core
from infrastructure.ecs_stack import Fargate
import os
import json
from infrastructure.emr_launch.cluster_definition import EMRClusterDefinition
from infrastructure.emr_orchestration.stack import StepFunctionStack
from infrastructure.emr_trigger.stack import EmrTriggerStack


# Load config
project_dir = os.path.dirname(os.path.abspath(__file__))

app = core.App()
config = app.node.try_get_context('config')
stack_id = config["stack-id"]


def fargate(config, stack_id):
    image_name = config["image-name"]
    ecr_repository_name = config["ecr-repository-name"]

    cpu = config["cpu"]
    memory_limit_mib = config["memory-limit-mib"]
    timeout_minutes = config["timeout-minutes"]
    s3_filters = config["s3-filters"]

    default_environment_vars = config["environment-variables"]
    input_bucket_name = config["input-bucket-name"]
    output_bucket_name = config["output-bucket-name"]
    topics_to_extract = ",".join(config["topics-to-extract"])

    fargate_stack = Fargate(
        app,
        stack_id,
        image_name=image_name,
        environment_vars=default_environment_vars,
        ecr_repository_name=ecr_repository_name,
        cpu=cpu,
        memory_limit_mib=memory_limit_mib,
        timeout_minutes=timeout_minutes,
        s3_filters=s3_filters,
        input_bucket_name=input_bucket_name,
        output_bucket_name=output_bucket_name,
        topics_to_extract=topics_to_extract,
        glue_db_name=config["glue-db-name"],
    )

    return fargate_stack


def emr(config, input_buckets: [str]):

    environment_variables = [
        "CLUSTER_NAME",
        "MASTER_INSTANCE_TYPE",
        "CORE_INSTANCE_TYPE",
        "CORE_INSTANCE_COUNT",
        "CORE_INSTANCE_MARKET",
        "TASK_INSTANCE_TYPE",
        "TASK_INSTANCE_COUNT",
        "TASK_INSTANCE_MARKET",
        "RELEASE_LABEL",
        "APPLICATIONS",
        "CONFIGURATION",
    ]

    list_vars = ["APPLICATIONS"]

    int_vars = [
        "CORE_INSTANCE_COUNT",
        "TASK_INSTANCE_COUNT",
    ]

    json_vars = ["CONFIGURATION"]

    clean_config = {"INPUT_BUCKETS": input_buckets}

    for v in environment_variables:
        val = config[v]
        clean_config[v] = val

    return EMRClusterDefinition(
        app, id=config["CLUSTER_NAME"] + "-stack", config=clean_config
    )


fargate_stack = fargate(config["fargate"], stack_id)

print("Output bucket: " + fargate_stack.output_bucket.bucket_arn)
emr_cluster_stack = emr(
    config["emr"], input_buckets=[fargate_stack.output_bucket.bucket_arn]
)

emr_orchestration_stack = StepFunctionStack(
    app,
    id=f"{stack_id}-emr-orchestration",
    emr_launch_stack=emr_cluster_stack,
    artifact_bucket=emr_cluster_stack.artifact_bucket,
    synchronized_bucket=emr_cluster_stack.synchronized_bucket,
    scenes_bucket=emr_cluster_stack.scenes_bucket,
    glue_db_name=config["fargate"]["glue-db-name"],
)


emr_trigger_stack = EmrTriggerStack(
    app,
    id=f"{stack_id}-emr-trigger",
    target_step_function_arn=emr_orchestration_stack.state_machine.state_machine_arn,
    source_bucket_sns=fargate_stack.new_files_topic,
    dynamo_table=emr_orchestration_stack.dynamo_table,
    num_rosbag_topics=len(config["fargate"]["topics-to-extract"]),
)

app.synth()
