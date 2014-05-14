#!/bin/bash
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function die() {
  # Message to STDERR goes to start-up script log in the instance.
  echo
  echo "########## ERROR ##########"
  echo "$@"
  echo "###########################"

  exit 1
}

declare -r METADATA_ROOT=http://metadata/computeMetadata/v1

function get_metadata_value() {
  path=$1
  curl --silent -f -H 'X-Google-Metadata-Request: True' $METADATA_ROOT/$path
}

function get_custom_metadata() {
  name=$1
  get_metadata_value "instance/attributes/$name"
}

NUM_WORKERS=$(get_custom_metadata 'num-workers')
HADOOP_MASTER=$(get_custom_metadata 'hadoop-master')
WORKER_NAME_TEMPLATE=$(get_custom_metadata 'hadoop-worker-template')
TMP_CLOUD_STORAGE=$(get_custom_metadata 'tmp-cloud-storage')
CUSTOM_COMMAND=$(get_custom_metadata 'custom-command')
DATA_DISK_ID=$(get_custom_metadata 'data-disk-id')

THIS_HOST=$(get_metadata_value  \
    instance/network-interfaces/0/access-configs/0/external-ip)
if [[ ! "$THIS_HOST" ]] ; then
  THIS_HOST=$(hostname)
fi

# Set up routing on master on cluster with no external IP address on workers.
if (( ! $(get_custom_metadata 'worker-external-ip') )) &&  \
    [[ "$(hostname)" == "$HADOOP_MASTER" ]] ; then
  echo "Setting up Hadoop master as Internet gateway for workers."
  # Turn on IP forwarding on kernel.
  perl -pi -e 's/#net.ipv4.ip_forward=1/net.ipv4.ip_forward=1/' /etc/sysctl.conf
  /sbin/sysctl -p
  # Set up NAT (IP masquerade) rule.
  iptables -t nat -A POSTROUTING -s 10.0.0.0/8 -j MASQUERADE
fi

# Increase fd limit
ulimit -n 32768
echo hadoop soft nofile 32768 >> /etc/security/limits.conf
echo hadoop hard nofile 32768 >> /etc/security/limits.conf

# Mount ephemeral disk
declare -r HADOOP_ROOT=/hadoop
declare -r DISK_DEVICE=/dev/disk/by-id/google-$DATA_DISK_ID

mkdir $HADOOP_ROOT
/usr/share/google/safe_format_and_mount $DISK_DEVICE $HADOOP_ROOT

# Set up user and group
groupadd --gid 5555 hadoop
useradd --uid 1111 --gid hadoop --shell /bin/bash -m hadoop

# Prepare directories
mkdir $HADOOP_ROOT/hdfs
mkdir $HADOOP_ROOT/hdfs/name
mkdir $HADOOP_ROOT/hdfs/data
mkdir $HADOOP_ROOT/checkpoint
mkdir $HADOOP_ROOT/mapred
mkdir $HADOOP_ROOT/mapred/history

chown -R hadoop:hadoop $HADOOP_ROOT
chmod -R 755 $HADOOP_ROOT

mkdir /run/hadoop
chown hadoop:hadoop /run/hadoop
chmod g+w /run/hadoop

declare -r HADOOP_LOG_DIR=/var/log/hadoop
mkdir $HADOOP_LOG_DIR
chgrp hadoop $HADOOP_LOG_DIR
chmod g+w $HADOOP_LOG_DIR

# Error check in CreateTrackerDirIfNeeded() in gslib/util.py in gsutil 3.37
# (line 119) raises exception when called from Hadoop streaming MapReduce,
# saying permission error to create /homes.
perl -pi -e '$.>110 and $.<125 and s/raise$/pass/'  \
    /usr/local/share/google/gsutil/gslib/util.py

declare -r TMP_DIR=/tmp/hadoop_package
declare -r HADOOP_DIR=hadoop-*
declare -r GENERATED_FILES_DIR=generated_files
declare -r DEB_PACKAGE_DIR=deb_packages

declare -r HADOOP_HOME=/home/hadoop
declare -r SCRIPT_DIR=hadoop_scripts

mkdir -p $TMP_DIR

# Set up SSH keys for hadoop user.
SSH_KEY_DIR=$HADOOP_HOME/.ssh
mkdir -p $SSH_KEY_DIR
get_custom_metadata 'hadoop-private-key' > $SSH_KEY_DIR/id_rsa
get_custom_metadata 'hadoop-public-key' > $SSH_KEY_DIR/authorized_keys

# Allow SSH between Hadoop cluster instances without user intervention.
SSH_CLIENT_CONFIG=$SSH_KEY_DIR/config
echo "Host *" >> $SSH_CLIENT_CONFIG
echo "  StrictHostKeyChecking no" >> $SSH_CLIENT_CONFIG

chown hadoop:hadoop -R $SSH_KEY_DIR
chmod 600 $SSH_KEY_DIR/id_rsa
chmod 700 $SSH_KEY_DIR
chmod 600 $SSH_CLIENT_CONFIG

# Download packages from Cloud Storage.
gsutil -m cp -R $TMP_CLOUD_STORAGE/$HADOOP_DIR.tar.gz  \
    $TMP_CLOUD_STORAGE/$DEB_PACKAGE_DIR  \
    $TMP_DIR ||  \
    die "Failed to download Hadoop and required packages from "  \
        "$TMP_CLOUD_STORAGE/"

gsutil cp $TMP_CLOUD_STORAGE/$GENERATED_FILES_DIR.tar.gz $TMP_DIR || die "Failed to download Hadoop and required packages from $TMP_CLOUD_STORAGE/"

tar zxf $TMP_DIR/$GENERATED_FILES_DIR.tar.gz

# Set up Java Runtime Environment.
dpkg -i --force-depends $TMP_DIR/$DEB_PACKAGE_DIR/*.deb

sudo mkdir /etc/ganglia/
sudo cp $TMP_DIR/$GENERATED_FILES_DIR/gmond.conf /etc/ganglia/gmond.conf
sudo sed -i 's/name = \"unspecified\"/name = \"GCE\"/' /etc/ganglia/gmond.conf
sudo sed -i "s/###HADOOP_MASTER###/$MASTER_NAME/g" /etc/ganglia/gmond.conf

sudo apt-get update
sudo apt-get -fy install
sudo DEBIAN_FRONTEND='noninteractive' apt-get -y --force-yes install unzip perl build-essential rrdtool librrds-perl librrd2-dev php5-gd libapr1-dev libconfuse-dev libdbi0-dev gcc python-dev python-setuptools

sudo easy_install -U pip
sudo pip uninstall crcmod
sudo pip install -U crcmod

wget "http://downloads.sourceforge.net/project/ganglia/ganglia%20monitoring%20core/3.6.0/ganglia-3.6.0.tar.gz?r=http%3A%2F%2Fsourceforge.net%2Fprojects%2Fganglia%2Ffiles%2Fganglia%2520monitoring%2520core%2F3.6.0%2F&ts=1392698981&use_mirror=ufpr" -O ganglia-3.6.0.tar.gz

tar zxf ganglia-3.6.0.tar.gz
ls
echo $HOSTNAME
# Setup ganglia-web if master node
if [ "$(hostname)" == "$HADOOP_MASTER" ] 
  then
  cd ganglia-3.6.0; ./configure --sysconfdir=/etc/ --with-gmetad
  make -C ~/ganglia-3.6.0/
  sudo make install -C ~/ganglia-3.6.0/
  sudo cp $TMP_DIR/$GENERATED_FILES_DIR/gmeta /etc/ganglia/gmetad.conf
  sudo gmond -c /etc/ganglia/gmond.conf
  echo "$HOSTNAME STARTED GMOND"
  sudo gmetad -c /etc/ganglia/gmetad.conf
  echo "$HOSTNAME STARTED GMETAD"
  sudo apt-get -fy install apache2 php5-mysql libapache2-mod-php5 php-pear php-xml-parser rrdtool screen #> /dev/null
  sudo gsutil cp $TMP_CLOUD_STORAGE/ganglia-web-3.5.10.tar.gz /home/hadoop/
  sudo tar zxf /home/hadoop/ganglia-web-3.5.10.tar.gz -C /home/hadoop/
  sudo mv /home/hadoop/ganglia-web-3.5.10/ /var/www/ganglia/
  sudo mkdir -p /var/lib/ganglia-web/dwoo/cache /var/lib/ganglia-web/dwoo/compiled
  sudo chown -R www-data:www-data /var/lib/ganglia-web/
  sudo chown -R www-data:www-data /var/www/ganglia/
  sudo cp /var/www/ganglia/conf_default.php /var/www/ganglia/conf.php
  sudo sed -i 's/8652/8651/' /var/www/ganglia/conf.php
  sudo mkdir -p /var/lib/ganglia/rrds/
  sudo chown -R nobody /var/lib/ganglia/
else
  cd ganglia-3.6.0; ./configure --sysconfdir=/etc/
  make -C ~/ganglia-3.6.0/
  sudo make install -C ~/ganglia-3.6.0/
  echo "$HOSTNAME STARTED GMOND"
  sudo gmond -c /etc/ganglia/gmond.conf
fi

sudo gsutil cp $TMP_CLOUD_STORAGE/crossbow-gce.tar.gz /home/hadoop/crossbow-gce.tar.gz

sudo tar zxf /home/hadoop/crossbow-gce.tar.gz -C /home/hadoop/
sudo chown -R hadoop:hadoop /home/hadoop/crossbow/

SCRIPT_AS_HADOOP=$TMP_DIR/setup_as_hadoop.sh
cat > $SCRIPT_AS_HADOOP <<NEKO
# Exits if one of the commands fails.
set -o errexit

HADOOP_CONFIG_DIR=\$HOME/hadoop/conf

# Extract Hadoop package.
tar zxf $TMP_DIR/$HADOOP_DIR.tar.gz -C \$HOME
ln -s \$HOME/$HADOOP_DIR \$HOME/hadoop

# Create masters file.
echo $HADOOP_MASTER > \$HADOOP_CONFIG_DIR/masters

# Create slaves file.
rm -f \$HADOOP_CONFIG_DIR/slaves
for ((i = 0; i < $NUM_WORKERS; i++)) ; do
  printf "$WORKER_NAME_TEMPLATE\n" \$i >> \$HADOOP_CONFIG_DIR/slaves
done

# Overwrite Hadoop configuration files.
perl -pi -e "s/###HADOOP_MASTER###/$HADOOP_MASTER/g"  \
    \$HADOOP_CONFIG_DIR/core-site.xml  \
    \$HADOOP_CONFIG_DIR/mapred-site.xml

perl -pi -e "s/###EXTERNAL_IP_ADDRESS###/$THIS_HOST/g"  \
    \$HADOOP_CONFIG_DIR/hdfs-site.xml  \
    \$HADOOP_CONFIG_DIR/mapred-site.xml

# Set PATH for hadoop user
echo "export PATH=\$HOME/hadoop/bin:\$HOME/hadoop/sbin:\\\$PATH" >>  \
    \$HOME/.profile
echo "export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64" >> \$HOME/.profile

NEKO

sudo -u hadoop bash $SCRIPT_AS_HADOOP ||  \
    die "Failed to run set-up command as hadoop user"

# Run custom commands.
eval "$CUSTOM_COMMAND" || die "Custom command error: $CUSTOM_COMMAND"

touch /home/george/complete

function run_as_hadoop() {
  failure_message=$1 ; shift

  sudo -u hadoop -i eval "$@" || die $failure_message
}

# Starts daemons if necessary.
function maybe_start_node() {
  condition=$1 ; shift
  failure_message=$1 ; shift

  if (( $(get_custom_metadata $condition) )) ; then
    run_as_hadoop "$failure_message" $@
  fi
}

# Starts NameNode and Secondary NameNode.  Format HDFS if necessary.
function start_namenode() {
  echo "Prepare and start NameNode(s)"

  run_as_hadoop "Failed to format HDFS" "echo 'Y' | hadoop namenode -format"

  # Start NameNode
  run_as_hadoop "Failed to start NameNode" hadoop-daemon.sh start namenode
  # Start Secondary NameNode
  run_as_hadoop "Failed to start Secondary NameNode" hadoop-daemon.sh start  \
      secondarynamenode
}

if (( $(get_custom_metadata NameNode) )) ; then
  start_namenode
fi

maybe_start_node DataNode "Failed to start DataNode"  \
    hadoop-daemon.sh start datanode

maybe_start_node JobTracker "Failed to start JobTracker"  \
    hadoop-daemon.sh start jobtracker

maybe_start_node TaskTracker "Failed to start TaskTracker"  \
    hadoop-daemon.sh start tasktracker

echo
echo "Start-up script for Hadoop finished."
echo
