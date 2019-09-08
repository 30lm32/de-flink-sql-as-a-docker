#!/usr/bin/env bash


# Creating Network If Not Exist
create_network_not_exist()
{
    n=`docker network ls | grep ${NETWORK_NAME} | wc -l`
    if [[ ${n} -eq 0 ]]
    then
        docker network create ${NETWORK_NAME}
        docker network ls
    fi
}

# Deleting Network If Exist
delete_network_exist()
{
    n=`docker network ls | grep ${NETWORK_NAME} | wc -l`
    if [[ ${n} -eq 1 ]]
    then
        docker network rm ${NETWORK_NAME}
    fi
}


if [[ ! ($# -ge 3 &&  $# -le 4) ]]
then
    echo "Usage: $0 <build|up|down> <docker_compose_file> <project_name> <app_env_shell(optional)>"
    exit
fi

COMMAND=$1
DOCKER_COMPOSE_FILE=$2
PROJECT_NAME=$3
APP_ENV_SHELL=$4

# Loading common environment variables for app and platform
. ./common_env.sh

# Loading app environment variables
if [[ ! -z ${APP_ENV_SHELL} ]]
then
    . ./${APP_ENV_SHELL}
fi


case ${COMMAND} in
    build)
        create_network_not_exist
        docker-compose -f ${DOCKER_COMPOSE_FILE} -p ${PROJECT_NAME} build;;
    up)
        create_network_not_exist
        docker-compose -f ${DOCKER_COMPOSE_FILE} -p ${PROJECT_NAME} up;;
    down)
        docker-compose -f ${DOCKER_COMPOSE_FILE} -p ${PROJECT_NAME} stop
        docker-compose -f ${DOCKER_COMPOSE_FILE} -p ${PROJECT_NAME} down
        docker-compose -f ${DOCKER_COMPOSE_FILE} -p ${PROJECT_NAME} rm
        delete_network_exist
        ;;
    ps)
        docker-compose -f ${DOCKER_COMPOSE_FILE} -p ${PROJECT_NAME} ps;;
    *)
        echo "Usage: $0 <build|up|down> <docker_compose_file> <project_name> <app_env_shell(optional)>";;
esac
