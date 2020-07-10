#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to compile Apache Doris(incubating)
# Usage:
#    sh build.sh        build both Backend and Frontend.
#    sh build.sh -clean clean previous output and build.
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=${ROOT}

. ${DORIS_HOME}/env.sh

# build thirdparty libraries if necessary
if [[ ! -f ${DORIS_THIRDPARTY}/installed/lib/libs2.a ]]; then
    echo "Thirdparty libraries need to be build ..."
    ${DORIS_THIRDPARTY}/build-thirdparty.sh
fi

PARALLEL=$[$(nproc)/4+1]

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --be               build Backend
     --fe               build Frontend and Spark Dpp application
     --spark-dpp        build Spark DPP application
     --clean            clean and build target
     --with-mysql       enable MySQL support(default)
     --without-mysql    disable MySQL support
     --with-lzo         enable LZO compress support(default)
     --without-lzo      disable LZO compress  support

  Eg.
    $0                                      build all
    $0 --be                                 build Backend without clean
    $0 --be --without-mysql                 build Backend with MySQL disable
    $0 --be --without-mysql --without-lzo   build Backend with both MySQL and LZO disable
    $0 --fe --clean                         clean and build Frontend and Spark Dpp application
    $0 --fe --be --clean                    clean and build Frontend, Spark Dpp application and Backend
    $0 --spark-dpp                          build Spark DPP application alone
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'be' \
  -l 'fe' \
  -l 'spark-dpp' \
  -l 'clean' \
  -l 'with-mysql' \
  -l 'without-mysql' \
  -l 'with-lzo' \
  -l 'without-lzo' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

BUILD_BE=
BUILD_FE=
BUILD_SPARK_DPP=
CLEAN=
RUN_UT=
WITH_MYSQL=ON
WITH_LZO=ON
HELP=0
if [ $# == 1 ] ; then
    # default
    BUILD_BE=1
    BUILD_FE=1
    BUILD_SPARK_DPP=1
    CLEAN=0
    RUN_UT=0
else
    BUILD_BE=0
    BUILD_FE=0
    BUILD_SPARK_DPP=0
    CLEAN=0
    RUN_UT=0
    while true; do
        case "$1" in
            --be) BUILD_BE=1 ; shift ;;
            --fe) BUILD_FE=1 ; shift ;;
            --spark-dpp) BUILD_SPARK_DPP=1 ; shift ;;
            --clean) CLEAN=1 ; shift ;;
            --ut) RUN_UT=1   ; shift ;;
            --with-mysql) WITH_MYSQL=ON; shift ;;
            --without-mysql) WITH_MYSQL=OFF; shift ;;
            --with-lzo) WITH_LZO=ON; shift ;;
            --without-lzo) WITH_LZO=OFF; shift ;;
            -h) HELP=1; shift ;;
            --help) HELP=1; shift ;;
            --) shift ;  break ;;
            *) ehco "Internal error" ; exit 1 ;;
        esac
    done
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

if [ ${CLEAN} -eq 1 -a ${BUILD_BE} -eq 0 -a ${BUILD_FE} -eq 0 -a ${BUILD_SPARK_DPP} -eq 0 ]; then
    echo "--clean can not be specified without --fe or --be or --spark-dpp"
    exit 1
fi

echo "Get params:
    BUILD_BE            -- $BUILD_BE
    BUILD_FE            -- $BUILD_FE
    BUILD_SPARK_DPP     -- $BUILD_SPARK_DPP
    CLEAN               -- $CLEAN
    RUN_UT              -- $RUN_UT
    WITH_MYSQL          -- $WITH_MYSQL
    WITH_LZO            -- $WITH_LZO
"

# Clean and build generated code
echo "Build generated code"
cd ${DORIS_HOME}/gensrc
if [ ${CLEAN} -eq 1 ]; then
   make clean
   rm -rf ${DORIS_HOME}/fe/fe-core/target
fi
# DO NOT using parallel make(-j) for gensrc
make
cd ${DORIS_HOME}

# Clean and build Backend
if [ ${BUILD_BE} -eq 1 ] ; then
    CMAKE_BUILD_TYPE=${BUILD_TYPE:-Release}
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR=${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}
    if [ ${CLEAN} -eq 1 ]; then
        rm -rf $CMAKE_BUILD_DIR
        rm -rf ${DORIS_HOME}/be/output/
    fi
    mkdir -p ${CMAKE_BUILD_DIR}
    cd ${CMAKE_BUILD_DIR}
    ${CMAKE_CMD} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DMAKE_TEST=OFF -DWITH_MYSQL=${WITH_MYSQL} -DWITH_LZO=${WITH_LZO} ../
    make -j${PARALLEL}
    make install
    cd ${DORIS_HOME}
fi

# Build docs, should be built before Frontend
echo "Build docs"
cd ${DORIS_HOME}/docs
./build_help_zip.sh
cd ${DORIS_HOME}

<<<<<<< HEAD
# Assesmble FE modules
FE_MODULES=
if [ ${BUILD_FE} -eq 1 -o ${BUILD_SPARK_DPP} -eq 1 ]; then
    if [ ${BUILD_SPARK_DPP} -eq 1 ]; then
        FE_MODULES="fe-common,spark-dpp"
    fi
    if [ ${BUILD_FE} -eq 1 ]; then
        FE_MODULES="fe-common,spark-dpp,fe-core"
    fi
fi

=======
which npm
>>>>>>> cmy first
# Clean and build Frontend
<<<<<<< HEAD
if [ ${FE_MODULES}x != ""x ]; then
    echo "Build Frontend Modules: $FE_MODULES"
=======
if [ ${BUILD_FE} -eq 1 ] ; then
    echo "Build Frontend UI"
    cd ${DORIS_HOME}/doris-ui
    npm install
    npm run build
    mkdir -p ${DORIS_HOME}/fe/src/main/resources/static
    cp -r ${DORIS_HOME}/doris-ui/dist/* ${DORIS_HOME}/fe/src/main/resources/static
    echo "Build Frontend"
>>>>>>> Add doris-ui automatic compilation script, automatically compile doris-ui when compiling FE
    cd ${DORIS_HOME}/fe
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean
    fi
<<<<<<< HEAD
    ${MVN_CMD} package -pl ${FE_MODULES} -DskipTests
=======
    ${MVN_CMD} package -DskipTests
    
>>>>>>> Modify the compilation script, copy spring boot application.yml to the be/conf directory, so that users can modify the http service port
    cd ${DORIS_HOME}
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/output/
mkdir -p ${DORIS_OUTPUT}

<<<<<<< HEAD
# Copy Frontend and Backend
if [ ${BUILD_FE} -eq 1 -o ${BUILD_SPARK_DPP} -eq 1 ]; then
    if [ ${BUILD_FE} -eq 1 ]; then
        install -d ${DORIS_OUTPUT}/fe/bin ${DORIS_OUTPUT}/fe/conf \
                   ${DORIS_OUTPUT}/fe/webroot/ ${DORIS_OUTPUT}/fe/lib/ \
                   ${DORIS_OUTPUT}/fe/spark-dpp/

        cp -r -p ${DORIS_HOME}/bin/*_fe.sh ${DORIS_OUTPUT}/fe/bin/
        cp -r -p ${DORIS_HOME}/conf/fe.conf ${DORIS_OUTPUT}/fe/conf/
        rm -rf ${DORIS_OUTPUT}/fe/lib/*
        cp -r -p ${DORIS_HOME}/fe/fe-core/target/lib/* ${DORIS_OUTPUT}/fe/lib/
        cp -r -p ${DORIS_HOME}/fe/fe-core/target/palo-fe.jar ${DORIS_OUTPUT}/fe/lib/
        cp -r -p ${DORIS_HOME}/docs/build/help-resource.zip ${DORIS_OUTPUT}/fe/lib/
        cp -r -p ${DORIS_HOME}/webroot/* ${DORIS_OUTPUT}/fe/webroot/
        cp -r -p ${DORIS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${DORIS_OUTPUT}/fe/spark-dpp/

    elif [ ${BUILD_SPARK_DPP} -eq 1 ]; then
        install -d ${DORIS_OUTPUT}/fe/spark-dpp/
        rm -rf ${DORIS_OUTPUT}/fe/spark-dpp/*
        cp -r -p ${DORIS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${DORIS_OUTPUT}/fe/spark-dpp/
    fi
=======
#Copy Frontend and Backend
if [ ${BUILD_FE} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/fe/bin ${DORIS_OUTPUT}/fe/conf \
               ${DORIS_OUTPUT}/fe/lib/

    cp -r -p ${DORIS_HOME}/bin/*_fe.sh ${DORIS_OUTPUT}/fe/bin/
    cp -r -p ${DORIS_HOME}/conf/fe.conf ${DORIS_OUTPUT}/fe/conf/
    rm -rf ${DORIS_OUTPUT}/fe/lib/*
    cp -r -p ${DORIS_HOME}/fe/target/lib/* ${DORIS_OUTPUT}/fe/lib/
    cp -r -p ${DORIS_HOME}/fe/target/palo-fe.jar ${DORIS_OUTPUT}/fe/lib/
    cp -r -p ${DORIS_HOME}/docs/build/help-resource.zip ${DORIS_OUTPUT}/fe/lib/
    cp -p  ${DORIS_HOME}/fe/src/main/resources/application.yml ${DORIS_OUTPUT}/fe/conf 
>>>>>>> Modify the compilation script, copy spring boot application.yml to the be/conf directory, so that users can modify the http service port
fi

if [ ${BUILD_BE} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/be/bin  \
               ${DORIS_OUTPUT}/be/conf \
               ${DORIS_OUTPUT}/be/lib/ \
               ${DORIS_OUTPUT}/be/www  \
               ${DORIS_OUTPUT}/udf/lib \
               ${DORIS_OUTPUT}/udf/include

    cp -r -p ${DORIS_HOME}/be/output/bin/* ${DORIS_OUTPUT}/be/bin/
    cp -r -p ${DORIS_HOME}/be/output/conf/* ${DORIS_OUTPUT}/be/conf/
    cp -r -p ${DORIS_HOME}/be/output/lib/* ${DORIS_OUTPUT}/be/lib/
    cp -r -p ${DORIS_HOME}/be/output/www/* ${DORIS_OUTPUT}/be/www/
    cp -r -p ${DORIS_HOME}/be/output/udf/*.a ${DORIS_OUTPUT}/udf/lib/
    cp -r -p ${DORIS_HOME}/be/output/udf/include/* ${DORIS_OUTPUT}/udf/include/
fi

echo "***************************************"
echo "Successfully build Doris"
echo "***************************************"

if [[ ! -z ${DORIS_POST_BUILD_HOOK} ]]; then
    eval ${DORIS_POST_BUILD_HOOK}
fi

exit 0
