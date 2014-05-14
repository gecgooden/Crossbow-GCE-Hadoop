#!/usr/bin/env python

import argparse
import re
import logging
import sys
import os
import subprocess
from time import gmtime, strftime, sleep

instances = {
  "n1-standard-1": 1,
  "n1-standard-2": 2,
  "n1-standard-4": 4,
  "n1-standard-8": 8,
  "n1-standard-16": 16,
  "n1-highmem-2": 2,
  "n1-highmem-4": 4,
  "n1-highmem-8": 8,
  "n1-highmem-16": 16,
  "n1-highcpu-2": 2,
  "n1-highcpu-4": 4,
  "n1-highcpu-8": 8,
  "n1-highcpu-16": 16,
  "f1-micro": 1,
  "g1-small": 1
}

MAX_ATTEMPTS = 10

class RunCrossbow(object):

  def __init__(self):
    self._parser = argparse.ArgumentParser()
  
  def CheckHadoopMasterReady(self, instance_name):
    command = ('gcutil ssh '
               '--ssh_arg "-o ConnectTimeout=10" '
               '--ssh_arg "-o StrictHostKeyChecking=no" '
               '%s cat complete') % (instance_name)
    print "Command: " + command
    if subprocess.call(command, shell=True):
      # Non-zero return code indicates an error.
      return False
    else:
      return True

  def CheckHadoopWorkerReady(self, master_name, instance_name):
    command = ('gcutil ssh '
               '--ssh_arg "-o ConnectTimeout=10" '
               '--ssh_arg "-o StrictHostKeyChecking=no" '
               '%s ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 %s cat complete') % (master_name, instance_name)
    print "Command: " + command
    if subprocess.call(command, shell=True):
      # Non-zero return code indicates an error.
      return False
    else:
      return True

  def _ParseArguments(self):
    parser = self._parser
    parser.add_argument('--project', help='Project ID to start GCE instances in.')
    parser.add_argument('--bucket', help='Google Cloud Storage bucket name for use')
    parser.add_argument('--manifest', help='The path of the manifest file to use (If --toUpload is false, then set this to the name of the file)')
    parser.add_argument('--referencejar', help='The path of the reference jar file to use (If --toUpload is false, then set this to the name of the file)')
    parser.add_argument('--debug', action='store_true', help='Use to enable debugging mode')
    parser.add_argument('--machinetype', default='n1-highcpu-8', help='Machine type of GCE instance.')
    parser.add_argument('--prefix', default='', help='Name Prefix of the GCE instances (default = "")')
    parser.add_argument('--noUpload', action='store_false', help='Should the manifest and reference jar be uploaded to Google Storage?')
    parser.add_argument('--numWorkers', default=1, help='The number of worker nodes in the cluster (default = 1)')
    parser.add_argument('--getLogs', action='store_true', help='Set if you would like to get the logs from the run')
    parser.add_argument('--dryrun', action='store_false', help='Set if you would like a dryrun only.')
    parser.add_argument('--setup', action='store_true', help='Used to setup Google Compute Environment')
    parser.add_argument('--disk_gb', default='500', help='Size in GB to use for instance persistent disk')
    parser.add_argument('--noauth_local_webserver', action='store_true', help='Do not attempt to open browser on local machine.')
    parser.add_argument('--external_ip_master', action='store_true', help='Uses only master for the external IP')
    parser.add_argument('--keepalive', action='store_false', help='Prevents the cluster from shutting down on completion (or failure) of the job. This is useful for debugging.')

  def exit(self, params):
    command = ('./compute_cluster_for_hadoop.py shutdown --prefix "%s" %s' % (params.prefix, params.project))
    if params.dryrun and params.keepalive:
      print "Command: " + command
      if subprocess.call(command, shell=True):
        logging.error("Error in stopping cluster, terminating...")
    sys.exit(1)


  def setup(self, params):
    print params
    if params.project == None or params.bucket == None:
      logging.error("Required parameters are missing. --project and --bucket are required.")
      sys.exit(1);

    print "Starting setup..."
    print "Downloading Required Packages..."
    hadoop_tar = 'http://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1.tar.gz'
    dependencies = ['http://security.debian.org/debian-security/pool/updates/main/o/openjdk-6/openjdk-6-jre-headless_6b27-1.12.6-1~deb7u1_amd64.deb', 
      'http://security.debian.org/debian-security/pool/updates/main/o/openjdk-6/openjdk-6-jre-lib_6b27-1.12.6-1~deb7u1_all.deb', 
      'http://http.us.debian.org/debian/pool/main/n/nss/libnss3-1d_3.14.3-1_amd64.deb',
      'http://http.us.debian.org/debian/pool/main/n/nss/libnss3_3.14.3-1_amd64.deb',
      'http://http.us.debian.org/debian/pool/main/c/ca-certificates-java/ca-certificates-java_20121112+nmu2_all.deb',
      'http://http.us.debian.org/debian/pool/main/n/nspr/libnspr4_4.9.2-1_amd64.deb']
    command = "curl -O %s" % hadoop_tar
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error Downloading %s" % hadoop_tar)
        sys.exit(1)
    

    if not os.path.exists("deb_packages"):
      command = "mkdir deb_packages"
      if params.dryrun:
        if subprocess.call(command, shell=True):
          logging.error("Error making directory deb_packages")
          sys.exit(1)

    for dep in dependencies:
      filename = dep.split('/')[-1]
      command = "curl -o deb_packages/%s %s" % (filename, dep)
      print "Command: " + command
      if params.dryrun:
        if subprocess.call(command, shell=True):
          logging.error("Error Downloading %s" % dep)
          sys.exit(1)

    print "Patching Hadoop..."
    command = "tar zxf %s && patch -p0 < hadoop-1.2.1.patch && tar zcf hadoop-1.2.1.tar.gz hadoop-1.2.1" % hadoop_tar.split('/')[-1]
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error Patching Hadoop")
        sys.exit(1)

    print "Setting up Client ID and Secret..."
    print 'Client ID and client secret can be set up from "APIs & auth" menu of Developers Console (https://cloud.google.com/console) of the project. Choose "Credentials" submenu, and click the red button at the top labeled "CREATE NEW CLIENT ID" to create a new pair of client ID and client secret for the application. Choose "Installed application" as "Application type", choose "Other" as "Installed application type" and click "Create Client ID" button.' 

    clientID = raw_input("Client ID: ").strip()
    clientSecret = raw_input("Client Secret: ").strip()

    command = "sed -i 's/{{{{ client_id }}}}/%s/g' gce_cluster.py && sed -i 's/{{{{ client_secret }}}}/%s/g' gce_cluster.py " % (clientID, clientSecret)
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error Configuring Client ID and Secret")
        sys.exit(1)

    print "Downloading and Setting up Python Libraries..."
    command = "curl -O http://google-api-python-client.googlecode.com/files/google-api-python-client-1.2.tar.gz && tar zxf google-api-python-client-1.2.tar.gz && ln -s google-api-python-client-1.2/apiclient . && ln -s google-api-python-client-1.2/oauth2client . && ln -s google-api-python-client-1.2/uritemplate ."
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error Configuring Google API Python Client")
        sys.exit(1)

    command = "curl -O https://httplib2.googlecode.com/files/httplib2-0.8.tar.gz && tar zxf httplib2-0.8.tar.gz && ln -s httplib2-0.8/python2/httplib2 ."
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error Configuring httplib2")
        sys.exit(1)

    command = "curl -O http://python-gflags.googlecode.com/files/python-gflags-2.0.tar.gz && tar zxf python-gflags-2.0.tar.gz && ln -s python-gflags-2.0/gflags.py . && ln -s python-gflags-2.0/gflags_validators.py ."
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error Configuring python-gflags")
        sys.exit(1)

    command = "curl -O https://pypi.python.org/packages/source/m/mock/mock-1.0.1.tar.gz && tar zxf mock-1.0.1.tar.gz && ln -s mock-1.0.1/mock.py ."
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error Configuring mock")
        sys.exit(1)

    print "Setting up Compute Cluster For Hadoop..."
    command = "./compute_cluster_for_hadoop.py setup %s %s" % (params.project, params.bucket) 
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error Setting up Compute Cluster For Hadoop")
        sys.exit(1)

    sys.exit(0)

  def ParseArgumentsAndExecute(self, argv):
    """Parses command-line arguments and executes sub-command handler."""
    self._ParseArguments()

    # Parse command-line arguments and execute corresponding handler function.
    params = self._parser.parse_args(argv)

    # Check prefix length.
    if hasattr(params, 'prefix') and params.prefix:
      # Prefix:
      #   - 15 characters or less.
      #   - May use lower case, digits or hyphen.
      #   - First character must be lower case alphabet.
      #   - May use hyphen at the end, since actual hostname continues.
      if not re.match('^[a-z][-a-z0-9]{0,14}$', params.prefix):
        logging.critical('Invalid prefix pattern.  Prefix must be 15 '
                         'characters or less.  Only lower case '
                         'alphabets, numbers and hyphen ("-") can be '
                         'used.  The first character must be '
                         'lower case alphabet.')
        sys.exit(1)

    print params
    if hasattr(params, 'setup') and params.setup:
      self.setup(params)
      sys.exit(1)
    elif params.project == None or params.bucket == None or params.referencejar == None or params.manifest == None or params.numWorkers == None:
      logging.error("Required parameters are missing. Please check your command and try again.")
      sys.exit(1);

    refJar = params.referencejar.split(os.sep)[-1]
    manifest = params.manifest.split(os.sep)[-1]

    if params.machinetype not in instances:
      print "Invalid machine type."
      sys.exit(1)

    numCpus=instances[params.machinetype]
    numInstances = int(params.numWorkers) + 1

    if params.noUpload:
      command = "gsutil cp %s gs://%s/crossbow-refs/%s && gsutil cp %s gs://%s/crossbow-refs/%s" % (params.referencejar, params.bucket, refJar, params.manifest, params.bucket, manifest)
      print "Command: " + command
      if params.dryrun:
        if subprocess.call(command, shell=True):
          logging.error("Error uploading to GS, terminating...")
          sys.exit(1)

    #create configs

    GENERATED_FILES_DIR = 'generated_files/'

    gmeta = open(GENERATED_FILES_DIR + "gmeta", 'w')
    gmeta_contents = ("data_source \"GCE\" %s-hm "% params.prefix) + " ".join([("%s-hw-%s") % (params.prefix, str(i).zfill(3)) for i in range(0, int(params.numWorkers))])
    gmeta.write(gmeta_contents + "\n")
    gmeta.close()

    command = "tar zcf generated_files.tar.gz %s && gsutil cp generated_files.tar.gz gs://%s/mapreduce/tmp/generated_files.tar.gz" % (GENERATED_FILES_DIR, params.bucket)
    if True: #params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error uploading generated files to Google Storage, terminating...")
        self.exit(params)


    command = ('./compute_cluster_for_hadoop.py')
    if params.noauth_local_webserver:
      command = command + " --noauth_local_webserver"
   
    command = command + (' start --machinetype %s --prefix "%s" %s %s %s' % (params.machinetype, params.prefix, params.project, params.bucket, params.numWorkers))
    
    if params.debug:
      command = command + " --debug"
    if params.external_ip_master:
      command = command + " --external-ip=master"
    if params.disk_gb:
      size = str(int(params.disk_gb)-10)
      command = command + (" --data-disk-gb %s" % size)
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error in starting cluster, terminating...")
        self.exit(params)

    if params.prefix <> '':
      master = params.prefix + '-hm'
    else:
      master = 'hm'

    ssh_command = "gcutil ssh %s" % (master)
    hadoop_ssh_command = '%s sudo sudo -u hadoop' % ssh_command
    command = '%s "gsutil cp gs://%s/crossbow-refs/%s /tmp/crossbow/%s && gsutil cp gs://%s/crossbow-refs/%s /tmp/crossbow/%s && mkdir /tmp/crossbow/output_full/"' %(ssh_command, params.bucket, refJar, refJar, params.bucket, manifest, manifest)
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error in downloading files to cluster, terminating...")
        self.exit(params)

    ## CHECKING STARTUP SCRIPTS ARE COMPLETE
    MASTER_NAME = "%s-hm"%params.prefix
    attempt_count = 0
    if params.dryrun:
      while not self.CheckHadoopMasterReady(MASTER_NAME):
        sleep(6)
        attempt_count = attempt_count + 1
        if attempt_count == MAX_ATTEMPTS:
          break;
      check_instances = [("%s-hw-%s") % (params.prefix, str(i).zfill(3)) for i in range(0, int(params.numWorkers))]
      while len(check_instances) != 0:
        for instance in check_instances:
          if self.CheckHadoopWorkerReady(MASTER_NAME, instance):
            check_instances.remove(instance)
            print instance, "removed"
          if len(check_instances) != 0:
            sleep(3)
        if len(check_instances) != 0:   
          sleep(3)

    command = '%s /home/hadoop/hadoop/bin/hadoop dfs -mkdir /crossbow-refs' % hadoop_ssh_command
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error connecting to HDFS, terminating...")
        self.exit(params)

    command = '%s /home/hadoop/hadoop/bin/hadoop dfs -put /tmp/crossbow/%s /crossbow-refs/%s' % (hadoop_ssh_command, refJar, refJar)
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error moving reference jar to HDFS, terminating...")
        self.exit(params)

    command = '%s /home/hadoop/hadoop/bin/hadoop dfs -put /tmp/crossbow/%s /crossbow-refs/%s' % (hadoop_ssh_command, manifest, manifest)
    print "Command: " + command
    if params.dryrun:
        if subprocess.call(command, shell=True): 
          logging.error("Error moving manifest to HDFS, terminating...")
          self.exit(params)

    command = '%s /home/hadoop/crossbow/crossbow-1.2.1/cb_hadoop --cpus=%s --instances=%s --hadoop=/home/hadoop/hadoop/bin/hadoop --bowtie=/home/hadoop/crossbow/bowtie-1.0.0/bowtie --soapsnp=/home/hadoop/crossbow/crossbow-1.2.1/soapsnp/soapsnp --fastq-dump=/home/hadoop/crossbow/sratoolkit.2.3.3-4-ubuntu64/bin/fastq-dump --preprocess --input=hdfs:///crossbow-refs/%s --output hdfs:///output_full/ --reference=hdfs:///crossbow-refs/%s | tee /tmp/hadoop_std.out' % (hadoop_ssh_command, numCpus, numInstances, manifest, refJar)
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Crossbow Job Failed, terminating...")
        self.exit(params)
   
    date = strftime("%Y-%m-%d_%H:%M:%S", gmtime())

    command = '%s /home/hadoop/hadoop/bin/hadoop dfs -get /output_full /home/hadoop/output_full' % (hadoop_ssh_command)
    print "Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error moving results to GS, terminating...")
        self.exit(params)    

    command = '%s gsutil -m cp -R /home/hadoop/output_full/ gs://%s/output_full/%s' % (ssh_command, params.bucket, date) 
    print"Command: " + command
    if params.dryrun:
      if subprocess.call(command, shell=True):
        logging.error("Error moving results to GS, terminating...")
        self.exit(params)   

    if params.getLogs:
      command = '%s gsutil -m cp -R /var/lib/ganglia/rrds/ gs://%s/rrds/%s' % (ssh_command, params.bucket, date) 
      print "Command: " + command
      if params.dryrun:
        if subprocess.call(command, shell=True):
          logging.error("Error moving RRDS to GS, terminating...")
          self.exit(params)

      command = '%s gsutil -m cp -R /var/log/hadoop/ gs://%s/hadoop_logs/%s' % (hadoop_ssh_command, params.bucket, date) 
      print "Command: " + command
      if params.dryrun:
        if subprocess.call(command, shell=True):
          logging.error("Error moving hadoop logs to GS, terminating...")
          self.exit(params)

      command = '%s gsutil -m cp -R /hadoop/tmp/mapred/local gs://%s/hadoop_logs_tmp/%s' % (hadoop_ssh_command, params.bucket, date) 
      print "Command: " + command
      if params.dryrun:
        if subprocess.call(command, shell=True):
          logging.error("Error moving hadoop logs to GS, terminating...")
          self.exit(params)

      command = '%s gsutil -m cp /tmp/hadoop_std.out gs://%s/hadoop_logs/%s/stdout/' % (hadoop_ssh_command, params.bucket, date) 
      print "Command: " + command
      if params.dryrun:
        if subprocess.call(command, shell=True):
          logging.error("Error moving hadoop logs to GS, terminating...")
          self.exit(params)

    self.exit(params)

def main():
  RunCrossbow().ParseArgumentsAndExecute(sys.argv[1:])

if __name__ == '__main__':
  main()
