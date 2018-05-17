#!/bin/bash -x

# Time marker for both stderr and stdout
date 1>&2

DEFAULT_PRESTO_HOME=/var/lib/presto
NODE_PROPERTIES_PATH=$DEFAULT_PRESTO_HOME/node.properties
JVM_DUMMY_CONFIG_PATH=$CONF_DIR/jvm.dummy.config
JVM_CONFIG_PATH=$CONF_DIR/etc/jvm.config
export JAVA_HOME=$CDH_PRESTO_JAVA_HOME
HIVE_CONF_PATH=$CONF_DIR/hive-conf
HBASE_CONF_PATH=$CONF_DIR/hbase-conf
PHOENIX_CONF_PATH=$CONF_DIR/phoenix-conf
IMPALA_CONF_PATH=$CONF_DIR/impala-conf

CMD=$1
PRESTO_CONF_FILE=$CONF_DIR/$2
PRESTO_CONF_PROP=$CONF_DIR/config.properties

#always load this plugin
ENABLED_PLUGINS=(raptor jmx resource-group-managers teradata-functions)
# option load plugins
[ "X${EVENT_LISTENER_ENABLED}" == "Xtrue" ] && ENABLED_PLUGINS+=(eventlistener-flume)
[ "X${CUSTOMIZED_AUTH_ENABLED}" == "Xtrue" ] && ENABLED_PLUGINS+=(auth-customized)
[ "X${HIVE_PLUGIN_ENABLED}" == "Xtrue" ] && ENABLED_PLUGINS+=(hive-hadoop2)
[ "X${ALLUXIO_PLUGIN_ENABLED}" == "Xtrue" ] && ENABLED_PLUGINS+=(hive-hadoop2)
[ "X${PHOENIX_PLUGIN_ENABLED}" != "Xnone" ] && ENABLED_PLUGINS+=(phoenix-${PHOENIX_PLUGIN_ENABLED})
[ "X${IMPALA_PLUGIN_ENABLED}" == "Xtrue" ] && ENABLED_PLUGINS+=(impala)
[ "X${MYSQL_PLUGIN_ENABLED}" == "Xtrue" ] && ENABLED_PLUGINS+=(mysql)
[ "X${TPCH_PLUGIN_ENABLED}" == "Xtrue" ] && ENABLED_PLUGINS+=(tpch)
[ "X${TPCDS_PLUGIN_ENABLED}" == "Xtrue" ] && ENABLED_PLUGINS+=(tpcds)




CATALOG_HIVE_PROP=$CONF_DIR/etc/catalog/hive.properties.tmp
CATALOG_PHOENIX_PROP=$CONF_DIR/etc/catalog/phoenix.properties.tmp
CATALOG_IMPALA_PROP=$CONF_DIR/etc/catalog/impala.properties.tmp
CATALOG_MYSQL_PROP=$CONF_DIR/etc/catalog/mysql.properties.tmp
CATALOG_TPCH_PROP=$CONF_DIR/etc/catalog/tpch.properties.tmp
CATALOG_TPCDS_PROP=$CONF_DIR/etc/catalog/tpcds.properties.tmp
CATALOG_ALLUXIO_PROP=$CONF_DIR/etc/catalog/alluxio.properties.tmp

RESOURCE_GROUPS_PROP=$CONF_DIR/etc/resource-groups.properties




function log {
  timestamp=$(date)
  echo "$timestamp: $1"	   #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

function generate_jvm_config {
  if [ -f $JVM_DUMMY_CONFIG_PATH ]; then
    cat $JVM_DUMMY_CONFIG_PATH | perl -e '$line = <STDIN>; chomp $line; $configs = substr($line, (length "jvm.config=")); for $value (split /\\n/, $configs) { print $value . "\n" }' > $JVM_CONFIG_PATH
  fi
}

function read_hadoop_site_property {
  local __file_name=$1
  local __prop_name=$2
  local __prop_value=`python - <<END
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
def getconfig(root, name):
  for existing_prop in root.getchildren():
    if existing_prop.find('name').text == name:
      return existing_prop.find('value').text

conf = ElementTree.parse("$__file_name").getroot()
prop_value = getconfig(root = conf, name = "$__prop_name")
print prop_value
END
`
  echo $__prop_value
}

function config_resource_groups {
    local __resource_groups_json="${CONF_DIR}/etc/resource_groups.json"
    # 查询资源分组没有配置，则使用默认的配置
    [ -s ${__resource_groups_json} ] || cp "${CONF_DIR}/aux/resource_groups.json" ${__resource_groups_json}
}

function enable_customized_eventlistener {
    if [ "X$EVENT_LISTENER_ENABLED" != "Xtrue" ]; then echo "Customized Event Listener feature is disables."; return; fi
    local __event_listener_prop="$CONF_DIR/etc/event-listener.properties.tmp"
    # 若定制查询事件监听器配置无效，则使用默认的
    [ -s ${__event_listener_prop} ] || cp "$CONF_DIR/aux/event-listener.properties" ${__event_listener_prop}
    ln -s ${__event_listener_prop} ${__event_listener_prop%.*}
}

function enable_customized_authorization {
  if [ "X$CUSTOMIZED_AUTH_ENABLED" != "Xtrue" ]; then echo "Customized Authorization feature is disables."; return; fi
  ln -s $CONF_DIR/access-control.properties.tmp $CONF_DIR/etc/access-control.properties
}

function enable_hive_conn_and_substitute_tokens {
  if [ "X${HIVE_PLUGIN_ENABLED}" != "Xtrue" ]; then echo "NOT Enable Plugin [hive-hadoop2]"; return; fi
  if [ ! -d "${HIVE_CONF_PATH}" ]; then echo "MUST be depends on Hive service"; return; fi
  if [ "X${HIVE_SECURITY}" != "Xnone" ]; then echo "hive.security=${HIVE_SECURITY}" >> $CATALOG_HIVE_PROP; fi
  local __hive_metastore_uri=$(read_hadoop_site_property "${HIVE_CONF_PATH}/hive-site.xml" "hive.metastore.uris" )
  sed -e "s#{{hive-conf}}#${HIVE_CONF_PATH}#g" \
    -e "s#{{hive-metastore-uri}}#${__hive_metastore_uri}#g" $CATALOG_HIVE_PROP > ${CATALOG_HIVE_PROP%.*}
}

function enable_alluxio_conn_and_substitute_tokens {
  if [ "X${ALLUXIO_PLUGIN_ENABLED}" != "Xtrue" ]; then echo "NOT Enable Plugin [alluxio]"; return; fi
  if [ ! -d "${HIVE_CONF_PATH}" ]; then echo "MUST be depends on Hive service"; return; fi

  local __hive_metastore_uri=$(read_hadoop_site_property "${HIVE_CONF_PATH}/hive-site.xml" "hive.metastore.uris" )
  sed -e "s#{{hive-conf}}#${HIVE_CONF_PATH}#g" \
    -e "s#{{hive-metastore-uri}}#${__hive_metastore_uri}#g" $CATALOG_ALLUXIO_PROP > ${CATALOG_ALLUXIO_PROP%.*}
}

function enable_phoenix_conn_and_substitute_tokens {
  if [ "X${PHOENIX_PLUGIN_ENABLED}" = "Xnone" ]; then echo "NOT Enable Plugin [phoenix]"; return; fi
  if [ ! -d "${HBASE_CONF_PATH}" ]; then echo "MUST be depends on HBase/Phoenix service"; return; fi

  local __zk_path=$(read_hadoop_site_property "${HBASE_CONF_PATH}/hbase-site.xml" "zookeeper.znode.parent")
  local __zk_quorum=$(read_hadoop_site_property "${HBASE_CONF_PATH}/hbase-site.xml" "hbase.zookeeper.quorum" )
  local __zk_port=$(read_hadoop_site_property "${HBASE_CONF_PATH}/hbase-site.xml" "hbase.zookeeper.property.clientPort")
#
#  local __zk_quorum_port=""
#  for h in `echo ${__zk_quorum} | tr "," "\n"`
#  do
#     __zk_quorum_port="${__zk_quorum_port},${h}:${__zk_port}"
#  done
#
  local __phoenix_connection_url="jdbc:phoenix:${__zk_quorum}:${__zk_port}:${__zk_path}"
  # :hbase@CHINANETCENTER.COM:/etc/security/keytab/hbszdx-hbase.keytab
  local __phoenix_connection_info="${HBASE_CONF_PATH}/core-site.xml,${HBASE_CONF_PATH}/hdfs-site.xml,${HBASE_CONF_PATH}/hbase-site.xml"
  if [ -f "${PHOENIX_CONF_PATH}/phoenix-site.xml" ]; then
    __phoenix_connection_info="${PHOENIX_CONF_PATH}/phoenix-site.xml,${__phoenix_connection_info}"
  fi


  sed -e "s#{{phoenix_version}}#${PHOENIX_PLUGIN_ENABLED}#g" \
    -e "s#{{phoenix_connection_url}}#${__phoenix_connection_url}#g" \
    -e "s#{{phoenix_connection_info}}#${__phoenix_connection_info}#g" $CATALOG_PHOENIX_PROP > ${CATALOG_PHOENIX_PROP%.*}
}

function enable_impala_conn_and_substitute_tokens {
  if [ "X${IMPALA_PLUGIN_ENABLED}" != "Xtrue" ]; then echo "NOT Enable Plugin [impala]"; return; fi
  if [ ! -d "${IMPALA_CONF_PATH}" ]; then echo "MUST be depends on Impala service"; return; fi

  local __hs2_port=$(grep "hs2_port" ${IMPALA_CONF_PATH}/impalad_flags | cut -f2 -d=)
  local __impalad=$(grep "hostname" ${IMPALA_CONF_PATH}/impalad_flags | cut -f2 -d=)
  local __impala_connection_url="jdbc:hive2://${__impalad}:${__hs2_port}/;"
  # principal=impala/hbszdx70@CHINANETCENTER.COM"
  local __impala_connection_info=""
  if [ -f "${IMPALA_CONF_PATH}/impala-site.xml" ]; then
    __impala_connection_info="${IMPALA_CONF_PATH}/impala-site.xml";
  fi
  sed -e "s#{{impala_connection_url}}#${__impala_connection_url}#g" \
    -e "s#{{impala_connection_info}}#${__impala_connection_info}#g" $CATALOG_IMPALA_PROP > ${CATALOG_IMPALA_PROP%.*}
}

function enable_other_conn {
    for arg in "$@"
    do
         eval "catalog_prop_tmp=(\${CATALOG_${arg^^}_PROP})"
         eval "x_plugin_enabled=(\${${arg^^}_PLUGIN_ENABLED})"
         echo "$x_plugin_enabled -> $catalog_prop_tmp"
         [ ${x_plugin_enabled:=false} == true ] && ( cat $catalog_prop_tmp > ${catalog_prop_tmp%.*}; )
    done
}

function modify_config_file {
    rm -f ${PRESTO_CONF_PROP}
    awk -F"=" '/query.max-memory/{print $1 "=" $2 "B" ;next}1' ${PRESTO_CONF_FILE} > ${PRESTO_CONF_PROP}
}

function link_files {
  cp -r $CDH_PRESTO_HOME/bin $CONF_DIR

  PRESTO_LIB=$CONF_DIR/lib
  if [ -L $PRESTO_LIB ]; then
    rm -rf $PRESTO_LIB
  fi
  ln -s $CDH_PRESTO_HOME/lib $PRESTO_LIB

  PRESTO_PLUGIN=$CONF_DIR/plugin
  if [ -d $PRESTO_PLUGIN ]; then
    rm -rf $PRESTO_PLUGIN
  fi
  mkdir $PRESTO_PLUGIN
  for p in ${ENABLED_PLUGINS[*]}
  do
      ln -s $CDH_PRESTO_HOME/plugin/$p $PRESTO_PLUGIN/
  done

  PRESTO_NODE_PROPERTIES=$CONF_DIR/etc/node.properties
  if [ -L $PRESTO_NODE_PROPERTIES ]; then
      rm -f $PRESTO_NODE_PROPERTIES
  fi
  ln -s $NODE_PROPERTIES_PATH $PRESTO_NODE_PROPERTIES
}

ARGS=()

case $CMD in

  (start_corrdinator)
    log "Startitng Presto Coordinator"
    link_files
    generate_jvm_config

    config_resource_groups
    enable_customized_eventlistener
    enable_customized_authorization

    enable_hive_conn_and_substitute_tokens
    enable_phoenix_conn_and_substitute_tokens
    enable_impala_conn_and_substitute_tokens
    enable_alluxio_conn_and_substitute_tokens
    enable_other_conn "mysql" "tpch" "tpcds"

    modify_config_file
    ARGS=("--config")
    ARGS+=("$PRESTO_CONF_PROP")
    ARGS+=("--data-dir")
    ARGS+=("$DEFAULT_PRESTO_HOME")
    ARGS+=("run")
    ;;

  (start_worker)
    log "Startitng Presto Worker"
    link_files
    generate_jvm_config

    enable_hive_conn_and_substitute_tokens
    enable_phoenix_conn_and_substitute_tokens
    enable_impala_conn_and_substitute_tokens
    enable_alluxio_conn_and_substitute_tokens
    enable_other_conn "mysql" "tpch" "tpcds"

    modify_config_file
    ARGS=("--config")
    ARGS+=("$PRESTO_CONF_PROP")
    ARGS+=("--data-dir")
    ARGS+=("$DEFAULT_PRESTO_HOME")
    ARGS+=("run")
    ;;

  (init_node_properties)
    if [ ! -f "$NODE_PROPERTIES_PATH" ]; then
      echo "node.environment=production" > $NODE_PROPERTIES_PATH
      echo "node.data-dir=/var/lib/presto" >> $NODE_PROPERTIES_PATH
      echo "node.id=`uuidgen`" >> $NODE_PROPERTIES_PATH
      log "create $NODE_PROPERTIES_PATH successfly"
    else
      log "$NODE_PROPERTIES_PATH is already created"
    fi
    exit 0

    ;;

  (*)
    log "Don't understand [$CMD]"
    ;;

esac

export PATH=$CDH_PRESTO_JAVA_HOME/bin:$PATH
cmd="$CONF_DIR/bin/launcher ${ARGS[@]}"
echo "Run [$cmd]"
exec $cmd
