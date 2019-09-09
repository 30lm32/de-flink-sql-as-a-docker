PLATFORM_DOCKER_COMPOSE_FILE:=docker-compose.flink.yml
APP_DOCKER_COMPOSE_FILE:=docker-compose.app-flink.yml
APP_ENV_SHELL=app_env.sh

build_platform:
	./dcompose.sh build ${PLATFORM_DOCKER_COMPOSE_FILE} platform
up_platform:
	./dcompose.sh up ${PLATFORM_DOCKER_COMPOSE_FILE} platform
down_platform:
	./dcompose.sh down ${PLATFORM_DOCKER_COMPOSE_FILE} platform
ps_platform:
	./dcompose.sh ps ${PLATFORM_DOCKER_COMPOSE_FILE} platform

mvn_test:
	mvn test
mvn_package:
	mvn clean package
build_app: mvn_package
	./dcompose.sh build ${APP_DOCKER_COMPOSE_FILE} app ${APP_ENV_SHELL}
up_app:
	./dcompose.sh up ${APP_DOCKER_COMPOSE_FILE} app ${APP_ENV_SHELL}
down_app:
	./dcompose.sh down ${APP_DOCKER_COMPOSE_FILE} app ${APP_ENV_SHELL}
ps_app:
	./dcompose.sh ps ${APP_DOCKER_COMPOSE_FILE} app ${APP_ENV_SHELL}


build_all: build_platform build_app
up_all: up_platform up_app
down_all: down_app down_platform
ps_all: ps_platform ps_app