import os

from aws_cdk import (
    core,
    aws_sns as sns,
    aws_iam,
    aws_stepfunctions as sfn,
    aws_s3_deployment as s3d,
    aws_dynamodb as dynamo,
    aws_glue as glue,
)


from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.step_functions import emr_chains
from aws_emr_launch.constructs.step_functions import emr_tasks


class StepFunctionStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        emr_launch_stack,
        artifact_bucket,
        synchronized_bucket,
        scenes_bucket,
        glue_db_name,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        launch_function = emr_launch_stack.launch_function

        # Create DynamoDB table for tracking
        dynamo_table = dynamo.Table(
            self,
            "dynamotable",
            table_name="Rosbag-EMR-Batch-Metadata",
            partition_key=dynamo.Attribute(
                name="BatchId", type=dynamo.AttributeType.STRING
            ),
            sort_key=dynamo.Attribute(name="Name", type=dynamo.AttributeType.STRING),
            billing_mode=dynamo.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY
        )

        dynamo_table_scenes = dynamo.Table(
            self,
            "dynamotablescenes",
            table_name="Rosbag-Scene-Metadata",
            partition_key=dynamo.Attribute(
                name="bag_file", type=dynamo.AttributeType.STRING
            ),
            sort_key=dynamo.Attribute(
                name="scene_id", type=dynamo.AttributeType.STRING
            ),
            billing_mode=dynamo.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY,
        )

        emr_role = aws_iam.Role.from_role_arn(
            self, "emr_role_iam", role_arn=emr_launch_stack.instance_role_arn
        )

        emr_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[dynamo_table.table_arn, dynamo_table_scenes.table_arn],
            )
        )

        emr_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "logs:CreateLogStream",
                    "logs:DescribeLogStreams",
                    "logs:CreateLogGroup",
                    "logs:PutLogEvents",
                    "ec2:DescribeTags",
                ],
                resources=["*"],
            )
        )

        # SNS Topics for Success/Failures messages from our Pipeline
        success_topic = sns.Topic(self, "SuccessTopic")
        failure_topic = sns.Topic(self, "FailureTopic")

        # Upload artifacts to S3
        step_code = s3d.BucketDeployment(
            self,
            id="sparkscript",
            destination_bucket=artifact_bucket,
            destination_key_prefix="steps",
            sources=[s3d.Source.asset("spark_scripts/")],
        )

        # Create a Chain to receive Failure messages
        fail = emr_chains.Fail(
            self,
            "FailChain",
            message=sfn.TaskInput.from_data_at("$.Error"),
            subject="Pipeline Failure",
            topic=failure_topic,
        )

        # # Define a Task to Terminate the Cluster on failure
        terminate_failed_cluster = emr_tasks.TerminateClusterBuilder.build(
            self,
            "TerminateFailedCluster",
            name="Terminate Failed Cluster",
            cluster_id=sfn.TaskInput.from_data_at(
                "$.LaunchClusterResult.ClusterId"
            ).value,
            result_path="$.TerminateResult",
        ).add_catch(fail, errors=["States.ALL"], result_path="$.Error")

        terminate_failed_cluster.next(fail)

        # Use a NestedStateMachine to launch the cluster
        launch_cluster = emr_chains.NestedStateMachine(
            self,
            "NestedStateMachine",
            name="Launch Cluster StateMachine",
            state_machine=launch_function.state_machine,
            fail_chain=fail,
        )

        synchronize = emr_chains.AddStepWithArgumentOverrides(
            self,
            "PySparkSynchronizeTopics",
            emr_step=emr_code.EMRStep(
                name=f"Synchronize Topics - PySpark Job",
                jar="command-runner.jar",
                args=[
                    "spark-submit",
                    "--master",
                    "yarn",
                    "--deploy-mode",
                    "cluster",
                    "--executor-cores",
                    "3",
                    os.path.join(
                        f"s3://{artifact_bucket.bucket_name}",
                        "steps",
                        "synchronize_topics.py",
                    ),
                    "--batch-id",
                    "DynamoDB.BatchId",
                    "--batch-metadata-table-name",
                    dynamo_table.table_name,
                    "--output-bucket",
                    synchronized_bucket.bucket_name,
                ],
            ),
            cluster_id=sfn.TaskInput.from_data_at(
                "$.LaunchClusterResult.ClusterId"
            ).value,
            result_path="$.PySparkResult",
            fail_chain=terminate_failed_cluster,
        )

        scene_detection = emr_chains.AddStepWithArgumentOverrides(
            self,
            "SceneDetection",
            emr_step=emr_code.EMRStep(
                name=f"Scene Detection - PySpark Job",
                jar="command-runner.jar",
                args=[
                    "spark-submit",
                    "--master",
                    "yarn",
                    "--deploy-mode",
                    "cluster",
                    "--executor-cores",
                    "3",
                    "--packages",
                    "com.audienceproject:spark-dynamodb_2.12:1.1.1",
                    os.path.join(
                        f"s3://{artifact_bucket.bucket_name}",
                        "steps",
                        "detect_scenes.py",
                    ),
                    "--batch-id",
                    "DynamoDB.BatchId",
                    "--batch-metadata-table-name",
                    dynamo_table.table_name,
                    "--input-bucket",
                    synchronized_bucket.bucket_name,
                    "--output-bucket",
                    scenes_bucket.bucket_name,
                    "--output-dynamo-table",
                    dynamo_table_scenes.table_name,
                ],
            ),
            cluster_id=sfn.TaskInput.from_data_at(
                "$.LaunchClusterResult.ClusterId"
            ).value,
            result_path="$.SceneResult",
            fail_chain=terminate_failed_cluster,
        )

        # Define a Task to Terminate the Cluster
        terminate_cluster = emr_tasks.TerminateClusterBuilder.build(
            self,
            "TerminateCluster",
            name="Terminate Cluster",
            cluster_id=sfn.TaskInput.from_data_at(
                "$.LaunchClusterResult.ClusterId"
            ).value,
            result_path="$.TerminateResult",
        ).add_catch(fail, errors=["States.ALL"], result_path="$.Error")

        # A Chain for Success notification when the pipeline completes
        success = emr_chains.Success(
            self,
            "SuccessChain",
            message=sfn.TaskInput.from_data_at("$.TerminateResult"),
            subject="Pipeline Succeeded",
            topic=success_topic,
        )

        # Assemble the Pipeline
        definition = (
            sfn.Chain.start(launch_cluster)
            .next(synchronize)
            .next(scene_detection)
            .next(terminate_cluster)
            .next(success)
        )

        # Create the State Machine
        self.state_machine = sfn.StateMachine(
            self,
            "SceneDetectionStateMachine",
            state_machine_name="scene-detection-pipeline",
            definition=definition,
        )
        self.dynamo_table = dynamo_table

        crawler_role = aws_iam.Role(
            self,
            "GlueCrawlerRole",
            managed_policies=[
                aws_iam.ManagedPolicy.from_managed_policy_arn(
                    self,
                    id="GlueService",
                    managed_policy_arn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
                ),
                aws_iam.ManagedPolicy.from_managed_policy_arn(
                    self,
                    id="S3Access",
                    managed_policy_arn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
                ),
            ],
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
        )

        crawler = glue.CfnCrawler(
            self,
            id="Crawler",
            name="synchronized_and_scenes",
            role=crawler_role.role_arn,
            database_name=glue_db_name,
            schedule=None,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://" + synchronized_bucket.bucket_name
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://" + scenes_bucket.bucket_name
                    ),
                ]
            ),
        )
