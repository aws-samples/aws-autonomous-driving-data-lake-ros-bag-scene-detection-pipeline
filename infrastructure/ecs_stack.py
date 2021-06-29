from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_efs as efs,
    aws_iam,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda,
    core,
    aws_logs,
    aws_sns as sns,
    aws_s3_notifications as s3n,
    aws_glue as glue,
    aws_dynamodb as dynamo,
)

from lambda_function import lambda_code
import boto3
import json

account = boto3.client("sts").get_caller_identity().get("Account")
region = boto3.session.Session().region_name


class Fargate(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        image_name: str,
        ecr_repository_name: str,
        environment_vars: dict,
        memory_limit_mib: int,
        cpu: int,
        timeout_minutes: int,
        s3_filters: list,
        glue_db_name: str,
        input_bucket_name: str,
        output_bucket_name: str,
        topics_to_extract: [str],
        **kwargs,
    ) -> None:
        """
        Creates the following infrastructure:

            2 S3 Buckets
                - "-in" bucket will be monitored for incoming data, and each incoming file will trigger an ECS Task
                - "-out" bucket will be the destination for saving processed data from the ECS Task

                - These bucket names are automatically passed as environment variables to your docker container
                    In your docker container, access these bucket names via:

                    import os
                    src_bucket = os.environ["s3_source"]
                    dest_bucket = os.environ["s3_destination"]


            ECS Fargate Cluster
                - Using Fargate, this cluster will not cost any money when no tasks are running

            ECS Fargate Task

            ECS Task Role - used by the docker container
                - Read access to the "-in" bucket and write access to the "-out" bucket

            VPC "MyVpc"
                Task will be run in this VPC's private subnets

            ECR Repository
                - reference to the repository hosting the service's docker image

            ECR Image
                - reference to the service's docker image in the ecr repo

            ECS Log Group for the ECS Task
                f'{image_name}-log-group'

            Step Function to execute the ECSRunFargateTask command

            Lambda Function listening for S3 Put Object events in src_bucket
                - then triggers Fargate Task for that object

        :param scope:
        :param id:
        :param image_name:
        :param image_dir:
        :param build_args:
        :param memory_limit_mib: RAM to allocate per task
        :param cpu: CPUs to allocate per task
        :param kwargs:
        """
        super().__init__(scope, id, *kwargs)

        src_bucket = aws_s3.Bucket(
            self,
            id=input_bucket_name,
            bucket_name=f"rosbag-file-ingest-{self.account}",
            removal_policy=core.RemovalPolicy.DESTROY,
        )

        dest_bucket = aws_s3.Bucket(
            self,
            id=output_bucket_name,
            bucket_name=f"rosbag-raw-topics-parquet-{self.account}",
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        self.output_bucket = dest_bucket

        # Create VPC and Fargate Cluster
        # NOTE: Limit AZs to avoid reaching resource quotas
        vpc = ec2.Vpc(
            self,
            f"MyVpc",
            max_azs=1,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="MyPrivateSubnet",
                    subnet_type=ec2.SubnetType.PRIVATE,
                    cidr_mask=27,
                ),
                ec2.SubnetConfiguration(
                    name="MyPublicSubnet",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=27,
                )
            ]
        )

        private_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE)

        # EFS
        fs = efs.FileSystem(
            self,
            "efs",
            vpc=vpc,
            removal_policy=core.RemovalPolicy.DESTROY,
            throughput_mode=efs.ThroughputMode.BURSTING,
            performance_mode=efs.PerformanceMode.MAX_IO,
        )

        access_point = fs.add_access_point(
            "AccessPoint",
            path="/ecs",
            create_acl=efs.Acl(owner_uid="1001", owner_gid="1001", permissions="750"),
            posix_user=efs.PosixUser(uid="1001", gid="1001"),
        )

        # Create DynamoDB table for tracking bag metadata
        dynamo_table = dynamo.Table(
            self,
            "dynamotable",
            table_name="Rosbag-BagFile-Metadata",
            partition_key=dynamo.Attribute(
                name="bag_file_prefix", type=dynamo.AttributeType.STRING
            ),
            billing_mode=dynamo.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY,
        )

        # ECS Task Role
        arn_str = "arn:aws:s3:::"

        ecs_task_role = aws_iam.Role(
            self,
            "ecs_task_role",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchFullAccess"
                )
            ],
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["dynamodb:*"], resources=[dynamo_table.table_arn]
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:Get*", "s3:List*"],
                resources=[
                    f"{arn_str}{src_bucket.bucket_name}",
                    f"{arn_str}{src_bucket.bucket_name}/*",
                ],
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:PutObject*"],
                resources=[
                    f"{arn_str}{dest_bucket.bucket_name}",
                    f"{arn_str}{dest_bucket.bucket_name}/*",
                ],
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["*"], resources=[access_point.access_point_arn]
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "elasticfilesystem:ClientMount",
                    "elasticfilesystem:ClientWrite",
                    "elasticfilesystem:DescribeMountTargets",
                ],
                resources=["*"],
            )
        )

        # Define task definition with a single container
        # The image is built & published from a local asset directory
        task_definition = ecs.FargateTaskDefinition(
            self,
            f"{image_name}_task_definition",
            family=f"{image_name}-family",
            cpu=cpu,
            memory_limit_mib=memory_limit_mib,
            task_role=ecs_task_role,
            volumes=[
                ecs.Volume(
                    name="efs-volume",
                    efs_volume_configuration=ecs.EfsVolumeConfiguration(
                        file_system_id=fs.file_system_id,
                        transit_encryption="ENABLED",
                        authorization_config=ecs.AuthorizationConfig(
                            access_point_id=access_point.access_point_id, iam="ENABLED"
                        ),
                    ),
                )
            ],
        )

        repo = ecr.Repository.from_repository_name(
            self, id=id, repository_name=ecr_repository_name
        )

        img = ecs.EcrImage.from_ecr_repository(repository=repo, tag="latest")

        logs = ecs.LogDriver.aws_logs(
            stream_prefix="ecs",
            log_group=aws_logs.LogGroup(
                self,
                f"{image_name}-log-group",
                log_group_name=f"/ecs/{image_name}",
                removal_policy=core.RemovalPolicy.DESTROY,
            ),
        )

        container_name = f"{image_name}-container"

        container_def = task_definition.add_container(
            container_name,
            image=img,
            memory_limit_mib=memory_limit_mib,
            environment={
                "s3_source": src_bucket.bucket_name,
                "s3_destination": dest_bucket.bucket_name,
                "topics_to_extract": topics_to_extract,
                "dynamo_table_name": dynamo_table.table_name,
            },
            logging=logs,
        )

        container_def.add_mount_points(
            ecs.MountPoint(
                container_path="/mnt/efs",
                source_volume="efs-volume",
                read_only=False,
            )
        )

        # Define an ECS cluster hosted within the requested VPC
        cluster = ecs.Cluster(
            self, "cluster", cluster_name=f"{image_name}-cluster", vpc=vpc
        )

        run_task = tasks.EcsRunTask(
            self,
            "fargatetask",
            assign_public_ip=False,
            subnets=private_subnets,
            cluster=cluster,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.VERSION1_4
            ),
            task_definition=task_definition,
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=task_definition.default_container,
                    environment=[
                        tasks.TaskEnvironmentVariable(
                            name=k, value=sfn.JsonPath.string_at(v)
                        )
                        for k, v in environment_vars.items()
                    ],
                )
            ],
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            input_path=sfn.JsonPath.entire_payload,
            output_path=sfn.JsonPath.entire_payload,
            timeout=core.Duration.minutes(timeout_minutes),
        )

        fs.connections.allow_default_port_from(run_task.connections)

        state_machine = sfn.StateMachine(
            self,
            "RunECSRosbagParser",
            state_machine_name="ECSRosbagParser",
            definition=run_task,
            timeout=core.Duration.minutes(timeout_minutes),
        )

        state_machine.grant_task_response(ecs_task_role)

        lambda_function = aws_lambda.Function(
            self,
            "StepFunctionTrigger",
            function_name="ECSTaskTrigger",
            code=aws_lambda.Code.from_inline(lambda_code),
            environment={
                "state_machine_arn": state_machine.state_machine_arn,
                "s3_prefixes": json.dumps(s3_filters.get("prefix", [])),
                "s3_suffixes": json.dumps(s3_filters.get("suffix", [])),
            },
            memory_size=3008,
            timeout=core.Duration.minutes(15),
            vpc=vpc,
            retry_attempts=0,
            handler="index.lambda_handler",
            runtime=aws_lambda.Runtime("python3.7", supports_inline_code=True),
        )

        lambda_function.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn],
            )
        )

        src_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,  # Event
            s3n.LambdaDestination(lambda_function),  # Dest
        )
        self.output_bucket_arn = dest_bucket.bucket_arn
        self.new_files_topic = sns.Topic(self, "NewFileEventNotification")
        dest_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED, s3n.SnsDestination(self.new_files_topic)
        )

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

        glue_db = glue.Database(self, "glue_db", database_name=glue_db_name)

        crawler = glue.CfnCrawler(
            self,
            id="Crawler",
            name="TopicParquetCrawler",
            role=crawler_role.role_arn,
            database_name=glue_db.database_name,
            schedule=None,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://" + dest_bucket.bucket_name
                    )
                ]
            ),
        )
