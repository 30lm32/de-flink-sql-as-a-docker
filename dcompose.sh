#!/usr/bin/env bash


create_network_not_exist()
{
    n=`docker network ls | grep ${NETWORK_NAME} | wc -l`
    if [ ${n} -eq 0  ]
    then
        # Creating Network
        docker network create ${NETWORK_NAME}
        docker network ls
    fi
}

delete_network_exist()
{
    n=`docker network ls | grep ${NETWORK_NAME} | wc -l`
    if [ ${n} -eq 1 ]
    then
        # Deleting Network
        docker network rm ${NETWORK_NAME}
    fi
}


if [[ ! ($# -ge 2 &&  $# -le 3) ]]
then
    echo "Usage: $0 <build|up|down> <docker_compose_file> <app_env_shell(optional)>"
    exit
fi

COMMAND=$1
DOCKER_COMPOSE_FILE=$2
APP_ENV_SHELL=$3

. ./common_env.sh

if [[ ! -z ${APP_ENV_SHELL} ]]
then
    . ./${APP_ENV_SHELL}
fi


case ${COMMAND} in
    build)
        create_network_not_exist
        docker-compose -f ${DOCKER_COMPOSE_FILE} build;;
    up)
        create_network_not_exist
        docker-compose -f ${DOCKER_COMPOSE_FILE} up;;
    down)
        docker-compose -f ${DOCKER_COMPOSE_FILE} down
        docker-compose -f ${DOCKER_COMPOSE_FILE} stop
        delete_network_exist
        ;;
    *) echo "Usage: $0 <build|up|down> <docker_compose_file>";;
esac



