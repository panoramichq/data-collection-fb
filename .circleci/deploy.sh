#!/usr/bin/env bash

# more bash-friendly output for jq
JQ="jq -r -e"

usage() {
    echo "Usage: $0 --cluster CLUSTER_NAME --service SERVICE_NAME --task TASK_NAME DOCKER_IMAGE"
    exit 1
}

configure_aws_cli(){
    aws configure set default.region $AWS_REGION
    aws configure set default.output json
}

deploy_cluster() {
    make_task_def
    register_definition

    if [[ $(aws ecs update-service --cluster $CLUSTER_NAME --service $SERVICE_NAME --task-definition $REVISION | \
                   $JQ '.service.taskDefinition') != $REVISION ]]; then
        echo "Error updating service."
        return 1
    fi

    # # wait for older revisions to disappear
    # # not really necessary, but nice for demos
    for attempt in {1..30}; do
        if STALE=$(aws ecs describe-services --cluster $CLUSTER_NAME --services $SERVICE_NAME | \
                       $JQ ".services[0].deployments | .[] | select(.taskDefinition != \"$REVISION\") | .taskDefinition"); then
            echo "Waiting for stale deployments:"
            echo "$STALE"
            sleep 5
        else
            echo "Deployed!"
            return 0
        fi
    done
    echo "Service update took too long."
    return 1
}

make_task_def(){
    echo "Get the previous task definition"
    OLD_TASK_DEF=$(aws ecs describe-task-definition --task-definition $TASK_NAME --output json)
    OLD_TASK_DEF_REVISION=$(echo $OLD_TASK_DEF | $JQ ".taskDefinition|.revision")

    # Update definition with new image
    NEW_TASK_DEF=$(echo $OLD_TASK_DEF | $JQ --arg NDI $DOCKER_IMAGE '.taskDefinition.containerDefinitions[0].image=$NDI')

    # Create new task JSON
    FINAL_TASK=$(echo $NEW_TASK_DEF | $JQ '.taskDefinition|{networkMode: .networkMode, family: .family, volumes: .volumes, executionRoleArn: .executionRoleArn, taskRoleArn: .taskRoleArn, containerDefinitions: .containerDefinitions, requiresCompatibilities: .requiresCompatibilities, cpu: .cpu, memory: .memory}')
}

register_definition() {

    if REVISION=$(aws ecs register-task-definition --cli-input-json "$(echo $FINAL_TASK)" --family $TASK_NAME | $JQ '.taskDefinition.taskDefinitionArn'); then
        echo "Revision: $REVISION"
    else
        echo "Failed to register task definition"
        return 1
    fi

}

while true ; do
    case "$1" in
        -t|--task) TASK_NAME=$2 ; shift 2 ;;
        -s|--service) SERVICE_NAME=$2 ; shift 2 ;;
        -c|--cluster) CLUSTER_NAME=$2 ; shift 2 ;;
        -h|--help) usage ;;
        --) shift ; break ;;
        *) break ;;
    esac
done

[ $# -eq 0 -o -z "$TASK_NAME" -o -z "$SERVICE_NAME" -o -z "$CLUSTER_NAME" ] && usage

DOCKER_IMAGE=$1
configure_aws_cli
deploy_cluster
