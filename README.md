Crossbow on Hadoop on Google Compute Engine
===========================================


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Summary
-------

This application sets up and configures Google Compute Engine instances to 
run [Crossbow](http://bowtie-bio.sourceforge.net/crossbow/index.shtml) on a Hadoop cluster.

The cluster created is non-persistent, meaning that any data needed is loaded from [Google Cloud Storage](https://developers.google.com/storage/) and the output data and logs are copied to Google Cloud Storage on completion of the job.

Prerequisites
-------------

The application assumes
[Google Cloud Storage](https://developers.google.com/storage/docs/signup) and
[Google Compute Engine](https://developers.google.com/compute/docs/signup)
services are enabled on the project.  The application requires sufficient
Google Compute Engine quota to run a Hadoop cluster.

### gcutil and gsutil

The application uses gcutil and gsutil, command line tools for
Google Compute Engine and Google Cloud Storage respectively.
The tools are distributed as a part of
[Google Cloud SDK](https://developers.google.com/cloud/sdk/).
Follow the instruction in the link to install them.

##### Authenticaton and default project

After the installation of Cloud SDK, run the following command to authenticate,
as instructed on the page.

    gcloud auth login

The above command authenticates the user, and sets the default project.
The default project must be set to the project where the Hadoop cluster will be
deployed.  The project ID is shown at the top of the project's "Overview" page
of the [Developers Console](https://cloud.google.com/console).

The default project can be changed with the command:

    gcloud config set project <project ID>

##### SSH key

The application uses SSH to start MapReduce tasks, on the Hadoop cluster.
In order for the application to execute remote commands automatically,
gcutil must have an SSH key with an empty passphrase for Google Compute Engine
instances.

gcutil uses its own
[SSH key](https://developers.google.com/compute/docs/instances#sshkeys),
and the key is stored in .ssh directory of the user's home directory as
`$HOME/.ssh/google_compute_engine` (private key) and
`$HOME/.ssh/google_compute_engine.pub` (public key).

If the SSH key for gcutil doesn't already exist, it can be created with
the command:

    ssh-keygen -t rsa -q -f $HOME/.ssh/google_compute_engine

Alternatively, the SSH key for gcutil can be generated upon the creation of an
instance.  The following command creates an instance, and an SSH key if it
doesn't exist on the computer.

    gcutil addinstance --zone us-central1-a --machine_type f1-micro  \
        --image debian-7-wheezy <instance-name>

The instance is created only to create an SSH key, and is safe to delete.

    gcutil deleteinstance -f --delete_boot_pd <instance-name>

If an SSH key with a passphrase already exists, the key files must be renamed
or deleted before creating the SSH key with an empty passphrase.

### Environment

The application runs with Python 2.7.
It's tested on Mac OS X and Linux.

Alternatively, a Google Compute Engine instance can be used to run the
application, which works as a controller of the Hadoop cluster.

### Google Storage Bucket

Create a Google Cloud Storage bucket, from which Google Compute Engine instance
downloads Hadoop, Crossbow and other packages.

This can be done by one of:

* Using an existing bucket.
* Creating a new bucket from the "Cloud Storage" page on the project page of
[Developers Console](https://cloud.google.com/console)
* Creating a new bucket by
[gsutil command line tool](https://developers.google.com/storage/docs/gsutil).
`gsutil mb gs://<bucket name>`


Security Issues
---------------

Without additional security consideration, which falls outside the scope
of the application, Hadoop's Web UI is open to public. This means that any confidential data you may have on the cluster is available without any authentication. You should take this into consideration before uploading your data to Google Cloud Storage.
Some resources on the Web are:
* [Authentication for Hadoop HTTP web-consoles](http://hadoop.apache.org/docs/stable/HttpAuthentication.html)
* [Google Compute Engine: Setting Up VPN Gateways](https://developers.google.com/compute/docs/networking#settingupvpn)


Set up Instructions
-------------------

This project has been modified to setup the environment and the required packages for you (If you would like manual instructions please refer to [Google's Readme](README.md.google)). 

To setup the environment, the Python script `./run_crossbow.py` is used with the following parameters:
    
    ./run_crossbow.py --setup --project <project> --bucket <bucket> [--dryrun]

Usage
-----

`./run_crossbow.py` supports the following arguments:

    ./run_crossbow.py --project <project> --bucket <bucket> --manifest <manifest> --referencejar <referencejar> \
        --machinetype <machinetype> --numWorkers <numWorkers> [--disk_gb <disk_gb>] [--prefix <prefix>] [--debug] \
        [--noUpload] [--getLogs] [--dryrun] [--noauth_local_webserver] [--external_ip_master] [--keepalive]

For more details about the arguments above, start `./run_crossbow.py` with the argument `-h`
