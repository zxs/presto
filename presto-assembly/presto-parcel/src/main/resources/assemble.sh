#!/bin/sh

pushd .
cd ${project.build.directory}
rm -rf ${parcel.name} repository
mv ${server.tar.package} ${parcel.name}

#<<COMMENT
jdk_download_url="http://download.oracle.com/otn-pub/java/jdk/${jdk.version}-${jdk.build}/jdk-${jdk.version}-linux-x64.tar.gz"
jdk_download_name="/tmp/jdk.tar.gz"
# curl -L -o $jdk_download_name -H "Cookie: oraclelicense=accept-securebackup-cookie" $jdk_download_url
decompressed_dir="extract"
mkdir $decompressed_dir
tar xzf $jdk_download_name -C $decompressed_dir
mv $decompressed_dir/$(\ls $decompressed_dir) ${parcel.name}/jdk
rm -rf $decompressed_dir
#COMMENT

#ln -s ${jdk8.home} ${parcel.name}/jdk

cat <<"EOF" > ${parcel.name}/bin/presto-cli
#!/usr/bin/env python

import os
import sys
import subprocess
from os.path import realpath, dirname

path = dirname(realpath(sys.argv[0]))
arg = ' '.join(sys.argv[1:])
cmd = "env PATH=\"%s/../jdk/bin:$PATH\" %s/presto-cli-${presto.version}-executable.jar %s" % (path, path, arg)

subprocess.call(cmd, shell=True)
EOF
chmod +x ${parcel.name}/bin/presto-cli-${presto.version}-executable.jar
chmod +x ${parcel.name}/bin/presto-cli

cp -a ${project.build.outputDirectory}/meta ${parcel.name}
tar zcf ${parcel.name}.parcel ${parcel.name}/ --owner=root --group=root

mkdir repository
for i in el5 el6 sles11 lucid precise squeeze wheezy; do
  cp ${parcel.name}.parcel repository/${parcel.name}-${i}.parcel
done

cd repository
curl https://raw.githubusercontent.com/cloudera/cm_ext/master/make_manifest/make_manifest.py | python

popd
