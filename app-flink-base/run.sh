#!/bin/bash

print_env_variables()
{
    echo "------------------------------------"
    echo "APP_PACKAGE: "${APP_PACKAGE}
    echo "APP_MAIN: "${APP_MAIN}
    echo "JOB_MANAGER_RPC_ADDRESS: "${JOB_MANAGER_RPC_ADDRESS}
    echo "JOB_MANAGER_RPC_PORT: "${JOB_MANAGER_RPC_PORT}
    echo "APP_JAR_NAME: "${APP_JAR_NAME}
    echo "------------------------------------"
}

if [ -z ${APP_PACKAGE} ] || [ -z ${APP_MAIN} ] || [ -z ${JOB_MANAGER_RPC_ADDRESS} ] || [ -z  ${JOB_MANAGER_RPC_PORT} ] || [ -z ${APP_JAR_NAME} ]
then
    echo "Some variable is empty!"
    print_env_variables
    exit
fi

APP_MAIN_CLASS=${APP_PACKAGE}.${APP_MAIN}
print_env_variables

#https://ci.apache.org/projects/flink/flink-docs-stable/ops/cli.html

#chown -R root:root target/

#ls -l

if [ -z ${APP_ARGS} ]
then
    flink run -m ${JOB_MANAGER_RPC_ADDRESS}:${JOB_MANAGER_RPC_PORT} -c ${APP_MAIN_CLASS} ./target/${APP_JAR_NAME}
else
    flink run -m ${JOB_MANAGER_RPC_ADDRESS}:${JOB_MANAGER_RPC_PORT} -c ${APP_MAIN_CLASS} ./target/${APP_JAR_NAME} ${APP_ARGS}
fi
