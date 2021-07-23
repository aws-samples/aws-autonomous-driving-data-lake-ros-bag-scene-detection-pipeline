
# Getting Started

Initial Configuration
    Define 3 names for your infrastructure in config.json:
    
    {
          "ecr-repository-name": "my-ecr-repository",
          "image-name": "my-image",
          "stack-id": "my-stack"
    }
     
   In deploy.sh, the REPO_NAME and IMAGE_NAME should match the values in your config.json
   
    REPO_NAME=my-ecr-repository # Should match the ecr repository name given in config.json
    IMAGE_NAME=my-image             # Should match the image name given in config.json

   
   Define other parameters for your Docker container, such as number of vCPUs and RAM it should consume, in config.json:
    
          "cpu": 4096,
          "memory-limit-mib": 12288,
          "timeout-minutes": 2
          "environment-variables": {}
   
   [Fargate CPU and Memory Limit Documentation](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html)
   

Extending the code to meet your use case:
    Edit the topics-to-extract list in the config.json. 
        These topics should all be in each rosbag file, as the emr pipeline will wait for all topics to arrive on s3 before processing the next batch 

    Extend the ./service/app/engine.py file to add more complex transformation logic
    
    Customizing Input
        Add prefix and suffix filters for the S3 notifications in config.json
        
    


deploy.sh with build=true will create an ecr repository in your account, if it does not yet exist, and push your docker image to that repository
Then it will execute the CDK command to deploy all infrastructure defined in app.py and ecs_stack.py 
          
          
The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the .env
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .env
```

After the init process completes and the virtualenv is created, you can use the following
step to test deployment

```
$ bash deploy.sh <cdk-command> <build?>

$ bash deploy.sh synth true
```


To add additional dependencies, for example other CDK libraries, just add
them to your `requirements.txt` or `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful CDK commands

 * `bash deploy.sh ls false`          list all stacks in the app
 * `bash deploy.sh synth false`       emits the synthesized CloudFormation template
 * `bash deploy.sh deploy true`      build and deploy this stack to your default AWS account/region
 * `bash deploy.sh diff true`        compare deployed stack with current state
 * `bash deploy.sh docs false`        open CDK documentation


## Topics in VSI Rosbag Files Data
             /as_tx/objects                         197 msgs    : derived_object_msgs/ObjectWithCovarianceArray
             /flir_adk/rgb_front_left/image_raw     198 msgs    : sensor_msgs/Image                            
             /flir_adk/rgb_front_right/image_raw    197 msgs    : sensor_msgs/Image                            
             /flir_adk/thermal/image_raw            195 msgs    : sensor_msgs/Image                            
             /gps                                   980 msgs    : visualization_msgs/Marker                    
             /imu_raw                               986 msgs    : sensor_msgs/Imu                              
             /muncaster/rgb/detections_only         197 msgs    : fusion/image_detections                      
             /muncaster/thermal/detections_only     197 msgs    : fusion/image_detections                      
             /nira_log/tgi                          490 msgs    : nira_log/tgi                                 
             /os1_cloud_node/points                 197 msgs    : sensor_msgs/PointCloud2                      
             /rosout                                 15 msgs    : rosgraph_msgs/Log                             (2 connections)
             /vehicle/brake_info_report             493 msgs    : dbw_mkz_msgs/BrakeInfoReport                 
             /vehicle/brake_report                  493 msgs    : dbw_mkz_msgs/BrakeReport                     
             /vehicle/fuel_level_report              98 msgs    : dbw_mkz_msgs/FuelLevelReport                 
             /vehicle/gear_report                   197 msgs    : dbw_mkz_msgs/GearReport                      
             /vehicle/gps/fix                         9 msgs    : sensor_msgs/NavSatFix                        
             /vehicle/gps/time                        9 msgs    : sensor_msgs/TimeReference                    
             /vehicle/gps/vel                         9 msgs    : geometry_msgs/TwistStamped                   
             /vehicle/imu/data_raw                  986 msgs    : sensor_msgs/Imu                              
             /vehicle/joint_states                 1974 msgs    : sensor_msgs/JointState                       
             /vehicle/misc_1_report                 196 msgs    : dbw_mkz_msgs/Misc1Report                     
             /vehicle/sonar_cloud                    33 msgs    : sensor_msgs/PointCloud2                      
             /vehicle/steering_report               988 msgs    : dbw_mkz_msgs/SteeringReport                  
             /vehicle/surround_report                33 msgs    : dbw_mkz_msgs/SurroundReport                  
             /vehicle/throttle_info_report          980 msgs    : dbw_mkz_msgs/ThrottleInfoReport              
             /vehicle/throttle_report               492 msgs    : dbw_mkz_msgs/ThrottleReport                  
             /vehicle/tire_pressure_report           19 msgs    : dbw_mkz_msgs/TirePressureReport              
             /vehicle/twist                         988 msgs    : geometry_msgs/TwistStamped                   
             /vehicle/wheel_position_report         491 msgs    : dbw_mkz_msgs/WheelPositionReport             
             /vehicle/wheel_speed_report            981 msgs    : dbw_mkz_msgs/WheelSpeedReport
             


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.