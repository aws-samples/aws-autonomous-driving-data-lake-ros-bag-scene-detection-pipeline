#!/bin/bash

cmd=$1
build=$2

REPO_NAME=ros-topic-extraction # Should match the ecr repository name given in config.json
IMAGE_NAME=ros-image          # Should match the image name given in config.json

python3 -m venv .env
source .env/bin/activate
pip install -r requirements.txt | grep -v 'already satisfied'

if [ $build = true ] ;
then
    docker build ./service -t $IMAGE_NAME:latest
    last_image_id=$(docker images | awk '{print $3}' | awk 'NR==2')
    account=$(aws sts get-caller-identity --query Account --output text)
    docker tag $last_image_id $account.dkr.ecr.eu-west-1.amazonaws.com/$REPO_NAME
    echo docker push $account.dkr.ecr.eu-west-1.amazonaws.com/$REPO_NAME
    aws ecr describe-repositories --repository-names $REPO_NAME || aws ecr create-repository --repository-name $REPO_NAME
    docker push $account.dkr.ecr.eu-west-1.amazonaws.com/$REPO_NAME
else
  echo Skipping build
fi

cdk $cmd --all --require-approval never