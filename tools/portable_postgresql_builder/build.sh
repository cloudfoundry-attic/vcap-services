#!/bin/bash -e

 function download() {
   url=$1
   to_path=$2
   echo "- Download $name from $url"
   wget -c --progress=dot --no-check-certificate $url $to_path 2>&1 | \
        while read line; do
            echo $line | grep "%" | sed -e "s/\.//g" | \
            awk '{printf("\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b%4s ETA: %6s", $2, $4)}'
        done
 }

 function handle_error() {
   exit_code=$?
   if [ ! $exit_code -eq 0 ];
   then
     echo "- Error! Check the logs/build.log"
     exit $exit_code
   fi
 }

 BASE_DIR=`dirname $0 | xargs realpath`
 source $BASE_DIR/config

 echo "Build from source code..."
 mkdir -p $BASE_DIR/logs
 rm -rf $BASE_DIR/logs/*
 BUILD_LOG=$BASE_DIR/logs/build.log
 mkdir -p $BASE_DIR/tmp
 rm -rf $BASE_DIR/tmp/*

 echo "- Install prequisites..."
 sudo apt-get install -y build-essential wget realpath 1>> $BUILD_LOG 2>&1
 handle_error

 DIST_DIR=$BASE_DIR/dist/$DIST_NAME

 pushd tmp
   mkdir -p $DIST_DIR
   if [ $FORCE_CLEAN -eq 1 ];
   then
     rm -rf $DIST_DIR/*
   fi
   for PKG in `echo $PKGS`; do   
     source $BASE_DIR/scripts/${PKG}/config
     if [ -e $DIST_DIR/$PKG_FILE ];
     then
        echo "- $PKG has been installed"
     else
       download ${SRC_URL} "-O ${PKG_NAME}.tar.gz"
       handle_error
       echo
       echo "- Uncompress the ${PKG_NAME} package"
       tar xzvf ${PKG_NAME}.tar.gz 1>>$BUILD_LOG 2>&1
       handle_error
       pushd $PKG_NAME
         echo "- Compile $PKG_NAME"
         cp $BASE_DIR/build_env ./.build_env
         cp $BASE_DIR/scripts/${PKG}/script ./.${PKG}_install_script
         chmod +x ./.${PKG}_install_script
         ./.${PKG}_install_script $DIST_DIR 1>>$BUILD_LOG 2>&1
         handle_error
         if [ -e $DIST_DIR/$PKG_FILE ];
         then
           echo " - $PKG_NAME compiled"
         else
           echo " - Failed to compile $PKG_NAME"
         fi
         if [ "$PKG" = "postgresql" ];
         then
            echo "- Postgresql is built successfully..."
         fi
       popd
     fi
   done
 popd

 pushd $DIST_DIR
   tar czvf $DIST_NAME.tar.gz * 1>>$BUILD_LOG 2>&1
   handle_error
   mkdir -p $BASE_DIR/release
   if [ -e $BASE_DIR/release/$DIST_NAME.tar.gz ];
   then
     rm -rf $BASE_DIR/release/$DIST_NAME.tar.gz
   fi
   cp $DIST_NAME.tar.gz $BASE_DIR/release
 popd
 rm -rf $DIST_DIR
 rm -rf tmp

