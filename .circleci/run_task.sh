#!/usr/bin/env bash

# more bash-friendly output for jq
JQ="jq -r -e"

usage() {
    echo "Usage: $0 --cluster CLUSTER_NAME --service SERVICE_NAME --task TASK_NAME COMMAND_LINE"
    exit 1
}

configure_aws_cli(){
    aws configure set default.region $AWS_REGION
    aws configure set default.output json
}

get_task_def(){
    echo "Get the previous task definition"
    OLD_TASK_DEF=$(aws ecs describe-task-definition --task-definition $TASK_NAME --output json)
    OLD_TASK_DEF_REVISION=$(echo $OLD_TASK_DEF | $JQ ".taskDefinition|.revision")

    NEW_CONTAINER_DEF=$(echo $OLD_TASK_DEF | $JQ '.taskDefinition.containerDefinitions[0]')

    echo "Get the previous service definition"
    SERVICE_DEF=$(aws ecs describe-services --cluster $CLUSTER_NAME --services $SERVICE_NAME | \
                    $JQ ".services[0].deployments | .[] | select(.status = \"PRIMARY\")")
    NETWORK_DEF=$(echo $SERVICE_DEF | $JQ '.networkConfiguration')
    LAUNCH_TYPE=$(echo $SERVICE_DEF | $JQ '.launchType')
}

run_task() {
    get_task_def

    CONTAINER_OVERRIDES=$(echo $NEW_CONTAINER_DEF | $JQ --argjson CMD $COMMAND_LINE '.|{command: $CMD, name: .name}')
    CONTAINER_COMP="{\"containerOverrides\":[$CONTAINER_OVERRIDES]}"

    echo "Running task"

    RUN_TASK_RESULT=$(aws ecs run-task --launch-type $LAUNCH_TYPE --cluster $CLUSTER_NAME --task-definition $TASK_NAME --overrides "$(echo $CONTAINER_COMP)" --network-configuration "$(echo $NETWORK_DEF)")

    echo $RUN_TASK_RESULT

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

COMMAND_LINE=$1
configure_aws_cli
run_task
