PLATFORM_DOCKER_COMPOSE_FILE:=docker-compose.flink.yml
APP_DOCKER_COMPOSE_FILE:=docker-compose.app-flink.yml
APP_ENV_SHELL=app_env.sh

build_platform:
	./dcompose.sh build ${PLATFORM_DOCKER_COMPOSE_FILE}
up_platform:
	./dcompose.sh up ${PLATFORM_DOCKER_COMPOSE_FILE}
stop_platform:
	./dcompose.sh down ${PLATFORM_DOCKER_COMPOSE_FILE}

mvn_package:
	mvn clean package

build_app: mvn_package
	./dcompose.sh build ${APP_DOCKER_COMPOSE_FILE} ${APP_ENV_SHELL}
up_app:
	./dcompose.sh up ${APP_DOCKER_COMPOSE_FILE} ${APP_ENV_SHELL}
stop_app:
	./dcompose.sh down ${APP_DOCKER_COMPOSE_FILE} ${APP_ENV_SHELL}

build_all: build_platform build_app
up_all: up_platform up_app
stop_all: stop_app stop_platform